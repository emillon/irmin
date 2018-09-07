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

module IO = Irmin_fs.IO_mem

let test_db = "test-db"

module Link = struct
  include Irmin_fs.Link(IO)(Irmin.Hash.SHA1)
  let v () = v (Irmin_fs.config test_db)
end

let init () =
  IO.clear () >|= fun () ->
  IO.set_listen_hook ()

let config = Irmin_fs.config test_db
let link = (module Link: Irmin_test.Link.S)
let clean () = Lwt.return_unit
let stats = None

let store =
  Irmin_test.store (module Irmin_fs.Make(IO)) (module Irmin.Metadata.None)

let suite = { Irmin_test.name = "FS"; init; clean; config; store; stats }
