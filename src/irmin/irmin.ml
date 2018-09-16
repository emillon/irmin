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

let src = Logs.Src.create "irmin" ~doc:"Irmin branch-consistent store"
module Log = (val Logs.src_log src : Logs.LOG)

module Type = Type
module Diff = Diff

module Contents = struct
  include Contents
  module type S0 = S.S0
  module type Conv = S.CONV
  module type S = S.CONTENTS
  module type STORE = S.CONTENTS_STORE
end
module Merge = Merge
module Branch = struct
  include Branch
  module type S = S.BRANCH
  module type STORE = S.BRANCH_STORE
end
module Info = Info
module Dot = Dot.Make
module Hash = struct
  include Hash
  module type S = S.HASH
end
module Path = struct
  include Path
  module type S = S.PATH
end

module S02Conv (C: Contents.S0): Contents.Conv with type t = C.t =
struct
  include C
  let pp = Type.pp_json C.t
  let of_string j = Type.decode_json C.t (Jsonm.decoder (`String j))
end

module Make_AO (AO: S.AO_MAKER) (K: S.HASH) (V: S.CONV) = struct
  type t = AO.t
  type key = K.t
  type value = V.t

  let v = AO.v

  let mem t k = AO.mem t (K.to_raw_string k)

  let find t k =
    AO.find t (K.to_raw_string k) >|= function
    | None   -> None
    | Some v -> match Type.decode_string V.t v with
      | Ok v          -> Some v
      | Error (`Msg e)->
        Fmt.invalid_arg "Cannot read the contents of %a: %s" K.pp k e

  let add t v =
    let v = Type.encode_string V.t v in
    let k = K.digest_string v in
    AO.add t (K.to_raw_string k) v >|= fun () ->
    k

end

module Make_RW (RW: S.RW_MAKER) (K: S.CONV) (V: S.HASH) = struct

  module W = Watch.Make(K)(V)
  module L = Lock.Make(K)

  type t = RW.t

  type watch = W.watch
  type key = K.t
  type value = V.t

  let watches = W.v ()
  let lock = L.v ()
  let v = RW.v

  let of_key = Fmt.to_to_string K.pp

  let to_key x = match K.of_string x with
    | Ok x -> x
    | Error (`Msg e) -> Fmt.invalid_arg "%s is not a valid key: %s" x e

  let of_value = V.to_raw_string

  let o f = function None -> None | Some x -> Some (f x)

  let mem t k = RW.mem t (of_key k)
  let list t = RW.list t >|= List.map to_key

  let set t k v =
    Log.debug (fun l -> l "RW.set %a" K.pp k);
    L.with_lock lock k (fun () -> RW.set t (of_key k) (of_value v)) >>= fun () ->
    W.notify watches k (Some v)

  let remove t k =
    Log.debug (fun l -> l "RW.remove %a" K.pp k);
    L.with_lock lock k (fun () -> RW.remove t (of_key k)) >>= fun () ->
    W.notify watches k None

  let find t k =
    Log.debug (fun l -> l "RW.find %a" K.pp k);
    RW.find t (of_key k) >|= function
    | None   -> None
    | Some v -> match Type.decode_string V.t v with
      | Ok v          -> Some v
      | Error (`Msg e)->
        Fmt.invalid_arg "Cannot read the contents of %a: %s" K.pp k e

  let test_and_set t k ~test ~set =
    Log.debug (fun l -> l "RW.test_and_set %a" K.pp k);
    L.with_lock lock k (fun () ->
        RW.test_and_set t (of_key k) ~test:(o of_value test) ~set:(o of_value set)
      ) >>= fun updated ->
    (if updated then W.notify watches k set else Lwt.return_unit) >|= fun () ->
     updated

  let watch _ = W.watch watches
  let watch_key _ = W.watch_key watches
  let unwatch _ = W.unwatch watches
end

module Make
    (AO: S.AO_MAKER)
    (RW: S.RW_MAKER)
    (M: S.METADATA)
    (C: S.CONTENTS)
    (P: S.PATH)
    (B: S.BRANCH)
    (H: S.HASH) =
struct

  module X = struct
    module XContents = struct
      include Make_AO (AO)(H)(C)
      module Key = H
      module Val = C
    end
    module Contents = Contents.Store(XContents)
    module Node = struct
      module AO = struct
        module Key = H
        module Val = Node.Make (H)(H)(P)(M)
        include Make_AO (AO)(Key)(S02Conv(Val))
      end
      include Node.Store(Contents)(P)(M)(AO)
    end
    module Commit = struct
      module AO = struct
        module Key = H
        module Val = Commit.Make (H)(H)
        include Make_AO (AO)(Key)(S02Conv(Val))
      end
      include Commit.Store(Node)(AO)
    end
    module Branch = struct
      module Key = B
      module Val = H
      include Make_RW (RW)(Key)(Val)
    end
    module Slice = Slice.Make(Contents)(Node)(Commit)
    module Sync = Sync.None(H)(B)
    module Repo = struct
      type t = {
        config: Conf.t;
        contents: Contents.t;
        node: Node.t;
        commit: Commit.t;
        branch: Branch.t;
      }
      let branch_t t = t.branch
      let commit_t t = t.commit
      let node_t t = t.node
      let contents_t t = t.contents

      let v config =
        AO.v config >>= fun t ->
        RW.v config >|= fun branch ->
        let contents = t in
        let node = contents, t in
        let commit = node, t in
        { contents; node; commit; branch; config }
    end
  end
  include Store.Make(X)
end

module Make_ext = Store.Make

module type RO = S.RO
module type AO = S.AO
module type LINK = S.LINK
module type RW = S.RW
module type TREE = S.TREE
module type S = S.STORE

type config = Conf.t
type 'a diff = 'a Diff.t

module type AO_MAKER = S.AO_MAKER

module type LINK_MAKER = S.LINK_MAKER

module type RW_MAKER = S.RW_MAKER
module type S_MAKER = S.MAKER

module type KV =
  S with type key = string list
     and type step = string
     and type branch = string

module type KV_MAKER = functor (C: Contents.S) -> KV with type contents = C.t

module Private = struct
  module Conf = Conf
  module Node = struct
    include Node
    module type S = S.NODE
    module type GRAPH = S.NODE_GRAPH
    module type STORE = S.NODE_STORE
  end
  module Commit = struct
    include Commit
    module type S = S.COMMIT
    module type STORE = S.COMMIT_STORE
    module type HISTORY = S.COMMIT_HISTORY
  end
  module Slice = struct
    include Slice
    module type S = S.SLICE
  end
  module Sync = struct
    include Sync
    module type S = S.SYNC
  end
  module type S = S.PRIVATE
  module Watch = Watch
  module Lock = Lock
end

let version = Version.current

module type SYNC = S.SYNC_STORE
module Sync = Sync_ext.Make

type remote = S.remote

let remote_store (type t) (module M: S with type t = t) (t:t) =
  let module X = (M: S.STORE with type t = t) in
  Sync_ext.remote_store (module X) t

let remote_uri = Sync_ext.remote_uri

module Metadata = struct
  module type S = S.METADATA
  module None = Node.No_metadata
end

module Json_tree = Contents.Json_tree
