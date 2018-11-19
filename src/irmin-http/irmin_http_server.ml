(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix
open Irmin_http_common

module T = Irmin.Type

let to_json = Irmin.Type.to_json_string
let of_json = Irmin.Type.of_json_string

let src = Logs.Src.create "irmin.http-server" ~doc:"Irmin REST API server"
module Log = (val Logs.src_log src : Logs.LOG)

module type S = sig
  type repo
  type t
  val v: ?strict:bool -> repo -> t
end

module Make (HTTP: Cohttp_lwt.S.Server) (S: Irmin.S) = struct

  module Wm = struct
    module Rd = Webmachine.Rd
    module Clock = struct
      let now = fun () -> int_of_float (Unix.gettimeofday ())
    end
    include Webmachine.Make(HTTP.IO)(Clock)
  end

  module P = S.Private

  class virtual resource = object
    inherit [Cohttp_lwt.Body.t] Wm.resource
    method! finish_request rd =
      Wm.Rd.with_resp_headers (fun h ->
          Cohttp.Header.add h irmin_version Irmin.version
        ) rd
      |> Wm.continue ()
  end

  let parse_error rd str (`Msg e) =
    let err = Fmt.strf "Parse error %S: %s" str e in
    Wm.respond ~body:(`String err) 400 rd

  module type BACKEND = sig
    type t
    type key
    type value
    val mem: t -> key -> bool Lwt.t
    val find: t -> key -> value option Lwt.t
    val add: t -> key -> value -> unit Lwt.t
  end

  module AO
      (S: BACKEND)
      (K: Irmin.Type.S with type t = S.key)
      (V: Irmin.Type.S with type t = S.value) =
  struct

    let with_key rd f =
      match Irmin.Type.of_string K.t (Wm.Rd.lookup_path_info_exn "id" rd) with
      | Ok key  -> f key
      | Error _ -> Wm.respond 404 rd

    class item db = object(self)
      inherit resource

      method private to_json rd =
        with_key rd (fun key ->
            let str = Irmin.Type.to_string V.t in
            S.find db key >>= function
            | Some value -> Wm.continue (`String (str value)) rd
            | None       -> assert false
          )

      method! allowed_methods rd = Wm.continue [`GET; `HEAD; `POST] rd
      method content_types_accepted rd = Wm.continue [] rd

      method! resource_exists rd =
        with_key rd (fun key ->
            S.mem db key >>= fun mem ->
            Wm.continue mem rd
          )

      method! process_post rd =
        with_key rd (fun key ->
            Cohttp_lwt.Body.to_string rd.Wm.Rd.req_body >>= fun body ->
            match Irmin.Type.of_string V.t body with
            | Error e -> parse_error rd body e
            | Ok body ->
              (* XXX(samoht): probably not very optimal/efficient... *)
              S.add db key body >>= fun () ->
              let resp_body = `Empty in
              Wm.continue true { rd with Wm.Rd.resp_body }
          )

      method content_types_provided rd =
        Wm.continue [
          "application/json", self#to_json
        ] rd

    end

  end

  module RW (S: Irmin.RW)
      (K: Irmin.Type.S with type t = S.key)
      (V: Irmin.Type.S with type t = S.value) =
  struct

    class items db = object(self)
      inherit resource

      method! allowed_methods rd = Wm.continue [`GET; `HEAD] rd
      method content_types_accepted rd = Wm.continue [] rd

      method private to_json rd =
        S.list db >>= fun keys ->
        let json = to_json T.(list K.t) keys in
        Wm.continue (`String json) rd

      method content_types_provided rd =
        Wm.continue [
          "application/json", self#to_json
        ] rd

    end

    let with_key rd f =
      match Irmin.Type.of_string K.t rd.Wm.Rd.dispatch_path with
      | Ok x    -> f x
      | Error _ -> Wm.respond 404 rd

    class item db = object(self)
      inherit resource

      method private of_json rd =
        Cohttp_lwt.Body.to_string rd.Wm.Rd.req_body >>= fun body ->
        match of_json (set_t V.t) body with
        | Error e -> parse_error rd body e
        | Ok v    ->
          with_key rd (fun key ->
              match v.v with
              | Some v ->
                S.set db key v >>= fun () ->
                let resp_body = `String (to_json status_t "ok") in
                let rd = { rd with Wm.Rd.resp_body } in
                Wm.continue true rd
              | None ->
                S.test_and_set db key ~test:v.test ~set:v.set >>= fun b ->
                let resp_body = `String (to_json status_t (string_of_bool b)) in
                let rd = { rd with Wm.Rd.resp_body } in
                Wm.continue b rd
            )

      method private to_json rd =
        with_key rd (fun key ->
            let str = Irmin.Type.to_string V.t in
            S.find db key >>= function
            | Some value -> Wm.continue (`String (str value)) rd
            | None       -> assert false
          )

      method! resource_exists rd =
        with_key rd (fun key ->
            S.mem db key >>= fun mem ->
            Wm.continue mem rd
          )

      method! allowed_methods rd =
        Wm.continue [`GET; `HEAD; `PUT; `DELETE] rd

      method content_types_provided rd =
        Wm.continue [
          "application/json", self#to_json
        ] rd

      method content_types_accepted rd =
        Wm.continue [
          "application/json", self#of_json
        ] rd

      method! delete_resource rd =
        with_key rd (fun key ->
            S.remove db key >>= fun () ->
            let resp_body = `String (to_json status_t "ok") in
            Wm.continue true { rd with Wm.Rd.resp_body }
          )

    end

    class watches db = object(self)
      inherit resource

      method! allowed_methods rd = Wm.continue [`GET; `HEAD; `POST] rd
      method content_types_accepted rd = Wm.continue [] rd

      method private stream ?init () =
        let stream, push = Lwt_stream.create () in
        S.watch ?init db (fun key diff ->
            let v = to_json (event_t K.t V.t) (key, diff) in
            push (Some v);
            push (Some ",");
            Lwt.return_unit
          ) >|= fun w ->
        Lwt.async (fun () ->
            Lwt_stream.closed stream >>= fun () ->
            S.unwatch db w
          );
        push (Some "[");
        `Stream stream

      method! process_post rd =
        Cohttp_lwt.Body.to_string rd.Wm.Rd.req_body >>= fun body ->
        match of_json T.(list (init_t K.t V.t)) body with
        | Error e -> parse_error rd body e
        | Ok init ->
          self#stream ~init () >>= fun resp_body ->
          Wm.continue true { rd with Wm.Rd.resp_body }

      method private of_json rd =
        self#stream () >>= fun body ->
        Wm.continue body rd

      method content_types_provided rd =
        Wm.continue [
          "application/json", self#of_json
        ] rd

    end

    class watch db = object(self)
      inherit resource

      method! allowed_methods rd = Wm.continue [`GET; `HEAD; `POST] rd
      method content_types_accepted rd = Wm.continue [] rd

      method private stream ?init key =
        let stream, push = Lwt_stream.create () in
        S.watch_key ?init db key (fun diff ->
            let v = to_json (event_t K.t V.t) (key, diff) in
            push (Some v);
            push (Some ",");
            Lwt.return_unit
          ) >|= fun w ->
        Lwt.async (fun () ->
            Lwt_stream.closed stream >>= fun () ->
            S.unwatch db w
          );
        push (Some "[");
        `Stream stream

      method! process_post rd =
        Cohttp_lwt.Body.to_string rd.Wm.Rd.req_body >>= fun body ->
        match of_json V.t body with
        | Error e -> parse_error rd body e
        | Ok init ->
          with_key rd (fun key ->
              self#stream ~init key >>= fun resp_body ->
              Wm.continue true { rd with Wm.Rd.resp_body }
            )

      method private of_json rd =
        with_key rd (fun key ->
            self#stream key >>= fun body ->
            Wm.continue body rd
          )

      method content_types_provided rd =
        Wm.continue [
          "application/json", self#of_json
        ] rd

    end

  end

  module Contents: BACKEND = struct
    type t = P.Repo.t
    type key = P.Contents.key
    type value = P.Contents.value
    let mem t k = P.Contents.mem (P.Repo.contents_t t) k
    let find t k = P.Content.find (P.Repo.contents_t t) k
    let add t k v = P.Repo.batch t (fun t _ _ -> P.Contents.add t k v)
  end

  module Blob = AO(P.Contents)(P.Contents.Key)(P.Contents.Val)
  module Tree = AO(P.Node)(P.Node.Key)(P.Node.Val)
  module Commit = AO(P.Commit)(P.Commit.Key)(P.Commit.Val)
  module Branch = RW(P.Branch)(P.Branch.Key)(P.Branch.Val)

  type repo = S.Repo.t
  type t = HTTP.t

  let v ?strict:_ db =
    let blob = P.Repo.contents_t db in
    let tree = P.Repo.node_t db in
    let commit = P.Repo.commit_t db in
    let branch = P.Repo.branch_t db in
    let routes = [
      ("/blob/:id"  , fun () -> new Blob.item      blob);
      ("/tree/:id"  , fun () -> new Tree.item      tree);
      ("/commit/:id", fun () -> new Commit.item    commit);
      ("/branches"  , fun () -> new Branch.items   branch);
      ("/branch/*"  , fun () -> new Branch.item    branch);
      ("/watches"   , fun () -> new Branch.watches branch);
      ("/watch/*"   , fun () -> new Branch.watch   branch);
    ] in
    let pp_con = Fmt.of_to_string Cohttp.Connection.to_string in
    let callback (_ch, conn) request body =
      let open Cohttp in
      Log.debug (fun l -> l "new connection %a" pp_con conn);
      (Wm.dispatch' routes ~body ~request >|= function
        | None        -> (`Not_found, Header.init (), `String "Not found", [])
        | Some result -> result)
      >>= fun (status, headers, body, path) ->
      Log.info (fun l ->
          l "[%a] %d - %s %s"
            pp_con conn
            (Code.code_of_status status)
            (Code.string_of_method (Request.meth request))
            (Uri.path (Request.uri request)));
      Log.debug (fun l ->
          l "[%a] path=%a"
            pp_con conn
            Fmt.(Dump.list string) path);
      (* Finally, send the response to the client *)
      HTTP.respond ~headers ~body ~status ()
    in
    (* create the server and handle requests with the function defined above *)
    let conn_closed (_, conn) =
      Log.debug (fun l -> l "connection %a closed" pp_con conn);
    in
    HTTP.make ~callback ~conn_closed ()

end
