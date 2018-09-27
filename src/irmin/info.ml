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

type t = {
  date   : int64;
  author : string;
  extra  : (string * string) list;
  message: string;
}

let t =
  let open Type in
  record "info" (fun date author extra message -> { date; author; message; extra })
  |+ field "date"    int64  (fun t -> t.date)
  |+ field "author"  string (fun t -> t.author)
  |+ field "extra"  (list ~len:`Int8 (pair string string)) (fun t -> t .extra)
  |+ field "message" string (fun t -> t.message)
  |> sealr

type f = unit -> t

let create ~date ~author ?(extra=[]) message = { date; message; extra; author }
let with_message t message = { t with message }
let empty = { date=0L; author=""; extra=[]; message = "" }

let v ~date ~author ?extra message =
  if date = 0L && author = "" && message = "" && (extra = None || extra = Some [])
  then empty
  else create ~date ~author ?extra message

let date t = t.date
let author t = t.author
let message t = t.message
let extra t = t.extra
let none = fun () -> empty
