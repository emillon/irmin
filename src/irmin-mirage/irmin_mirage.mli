(*
 * Copyright (c) 2017 Thomas Gazagnaire <thomas@gazagnaire.org>
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

(** MirageOS backend, with bi-directional compatibility with Git *)

(** The context to use for synchronisation. *)

module Info (N: sig val name: string end)(C: Mirage_clock.PCLOCK): sig

  (** {1 Commit info creators} *)

  val f: C.t -> ('a, Format.formatter, unit, Irmin.Info.f) format4 -> 'a
  (** Commit info function, using [N.name] and [C.now_d_ps] provided
      in the functor arguments. *)

end

(** Functor to create a MirageOS' KV_RO store from a Git
    repository. *)
module KV_RO (G: Irmin_git.G): sig

  include Mirage_kv_lwt.RO

  val connect: ?depth:int -> ?branch:string -> ?path:string ->
    G.t -> Uri.t -> t Lwt.t
  (** [connect ?depth ?branch ?path g uri] clones the given [uri] into
     [g] repository, using the given [branch], [depth] and
     ['/']-separated sub-[path]. By default, [branch] is master,
     [depth] is [1] and [path] is empty, ie. reads will be relative to
     the root of the repository. *)

end

module Git: sig

  module AO (G: Git.S) (V: Irmin.Type.S):
    Irmin.AO.S with type t = G.t
                and type key = G.Hash.t
                and type value = V.t
  (** Embed an append-only store into a Git repository. Contents will
      be written in {i .git/objects/} and might be cleaned up if you
      run {i git gc} manually. *)

  module Make (C: Git_mirage.Net.CONDUIT): Irmin_git.S_MAKER
  (** Embed an Irmin store into an in-memory Git repository. *)

  module KV (C: Git_mirage.Net.CONDUIT): Irmin_git.KV_MAKER

  module Ref (C: Git_mirage.Net.CONDUIT): Irmin_git.REF_MAKER

end
