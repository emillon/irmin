(*
 * Copyright (c) 2013-2018 Thomas Gazagnaire <thomas@gazagnaire.org>
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

let src = Logs.Src.create "irmin" ~doc:"Irmin branch-consistent store"
module Log = (val Logs.src_log src : Logs.LOG)

module Make (RW: S.RW_MAKER) (K: Type.S) (V: S.HASH) = struct

  module W = Watch.Make(K)(V)
  module L = Lock.Make(K)

  type t = RW.t

  type watch = W.watch
  type key = K.t
  type value = V.t

  let watches = W.v ()
  let lock = L.v ()
  let v = RW.v

  let pp_key = K.pp
  let of_key = Type.encode_string K.t

  let to_key x = match Type.decode_string K.t x with
    | Ok x -> x
    | Error (`Msg e) -> Fmt.invalid_arg "%s is not a valid key: %s" x e

  let of_value = Type.encode_string V.t

  let o f = function None -> None | Some x -> Some (f x)

  let mem t k = RW.mem t (of_key k)
  let list t = RW.list t >|= List.map to_key

  let set t k v =
    Log.debug (fun l -> l "RW.set %a" pp_key k);
    L.with_lock lock k (fun () -> RW.set t (of_key k) (of_value v)) >>= fun () ->
    W.notify watches k (Some v)

  let remove t k =
    Log.debug (fun l -> l "RW.remove %a" pp_key k);
    L.with_lock lock k (fun () -> RW.remove t (of_key k)) >>= fun () ->
    W.notify watches k None

  let find t k =
    Log.debug (fun l -> l "RW.find %a" pp_key k);
    RW.find t (of_key k) >|= function
    | None   -> None
    | Some v -> match Type.decode_string V.t v with
      | Ok v          -> Some v
      | Error (`Msg e)->
        Fmt.invalid_arg "Cannot read the contents of %a: %s" pp_key k e

  let test_and_set t k ~test ~set =
    Log.debug (fun l -> l "RW.test_and_set %a" pp_key k);
    L.with_lock lock k (fun () ->
        RW.test_and_set t (of_key k) ~test:(o of_value test) ~set:(o of_value set)
      ) >>= fun updated ->
    (if updated then W.notify watches k set else Lwt.return_unit) >|= fun () ->
     updated

  let watch _ = W.watch watches
  let watch_key _ = W.watch_key watches
  let unwatch _ = W.unwatch watches
end
