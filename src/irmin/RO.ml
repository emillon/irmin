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

module Make (AO: S.RO_MAKER) (K: Type.S) (V: Type.S) = struct

  type t = AO.t
  type key = K.t
  type value = V.t

  let v = AO.v

  let key = Type.encode_string K.t
  let pp =  K.pp

  let mem t k = AO.mem t (key k)

  let find t k =
    AO.find t (key k) >|= function
    | None   -> None
    | Some v -> match Type.decode_string V.t v with
      | Ok v          -> Some v
      | Error (`Msg e)->
        Fmt.invalid_arg "Cannot read the contents of %a: %s" pp k e

end
