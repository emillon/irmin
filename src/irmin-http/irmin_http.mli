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

(** JSON REST/CRUD interface. *)

val config: ?config:Irmin.config -> Uri.t -> Irmin.config

val uri: Uri.t option Irmin.Private.Conf.key

module type CLIENT = sig
  include Cohttp_lwt.S.Client
  val ctx: unit -> ctx option
end

module type S = sig
  include Irmin.S
  type ctx
  val connect: ?ctx:ctx -> Uri.t -> Repo.t Lwt.t
end

module Make (Client: CLIENT)
    (M: Irmin.Metadata.S)
    (C: Irmin.Contents.S)
    (P:Irmin.Path.S)
    (B: Irmin.Branch.S)
    (H: Irmin.Hash.S):
  S with type key = P.t
     and type step = P.step
     and type metadata = M.t
     and type contents = C.t
     and type branch = B.t
     and type Commit.Hash.t = H.t
     and type Tree.Hash.t = H.t
     and type Contents.Hash.t = H.t
     and type ctx = Client.ctx

module type KV = sig
  include Irmin.KV
  type ctx
  val connect: ?ctx:ctx -> Uri.t -> Repo.t Lwt.t
end

module KV (Client: CLIENT) (C: Irmin.Contents.S):
  KV with type contents = C.t
      and type ctx = Client.ctx
