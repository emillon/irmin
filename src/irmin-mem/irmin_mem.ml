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

let src = Logs.Src.create "irmin.mem" ~doc:"Irmin in-memory store"
module Log = (val Logs.src_log src : Logs.LOG)

module RO = struct

  module KMap = Map.Make(String)

  type key = string
  type value = string
  type t = { mutable t: value KMap.t }
  let map = { t = KMap.empty }
  let v _config = Lwt.return map

  let find pp { t; _ } key =
    Log.debug (fun f -> f "find %a" pp key);
    try Lwt.return (Some (KMap.find key t))
    with Not_found -> Lwt.return_none

  let mem pp { t; _ } key =
    Log.debug (fun f -> f "mem %a" pp key);
    Lwt.return (KMap.mem key t)

end


let pp_hex ppf x = let `Hex h = Hex.of_string x in Fmt.string ppf h

module AO = struct

  include RO

  let find = find pp_hex
  let mem = mem pp_hex

  let add t key value =
    Log.debug (fun f -> f "add %a" pp_hex key);
    t.t <- KMap.add key value t.t;
    Lwt.return ()

end

module Link = AO

module RW = struct

  include RO

  let pp = Fmt.string
  let find = find pp
  let mem = mem pp

  let list t =
    RO.KMap.fold (fun k _ acc -> k :: acc) t.t []
    |> Lwt.return

  let set t key value =
    t.t <- RO.KMap.add key value t.t;
    Lwt.return ()

  let remove t key =
    t.t <- RO.KMap.remove key t.t;
    Lwt.return_unit

  let test_and_set t key ~test ~set =
    find t key >>= fun v ->
    if Irmin.Type.(equal (option string)) test v then (
      let () = match set with
        | None   -> t.t <- RO.KMap.remove key t.t
        | Some v -> t.t <- RO.KMap.add key v t.t
      in
      Lwt.return true
    ) else
      Lwt.return false

end

let config () = Irmin.Private.Conf.empty

module Make = Irmin.Make(AO)(RW)

module KV (C: Irmin.Contents.S) =
  Make
    (Irmin.Metadata.None)
    (C)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Irmin.Hash.SHA1)
