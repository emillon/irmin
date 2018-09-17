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

module Type = Type
module Diff = Diff

module Contents = struct
  include Contents
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

module RO = struct
  module type S = S.RO
  module type MAKER = S.RO_MAKER
  module Make = RO.Make
end

module AO = struct
  module type S = S.AO
  module type MAKER = S.AO_MAKER
  module Make = AO.Make
end

module RW = struct
  module type S = S.RW
  module type MAKER = S.RW_MAKER
  module Make = RW.Make
end

module Link = struct
  module type S = S.LINK
  module type MAKER = S.LINK_MAKER
  module Make (S: MAKER) (K: Type.S) (V: Type.S) = struct
    include RO.Make(S)(K)(V)
    let add t k v = S.add t (Type.encode_bin K.t k) (Type.encode_bin V.t v)
  end
end

module Make
    (AOM: S.AO_MAKER)
    (RWM: S.RW_MAKER)
    (M: S.METADATA)
    (C: S.CONTENTS)
    (P: S.PATH)
    (B: S.BRANCH)
    (H: S.HASH) =
struct

  module X = struct
    module XContents = struct
      include AO.Make (AOM)(H)(C)
      module Key = H
      module Val = C
    end
    module Contents = Contents.Store(XContents)
    module Node = struct
      module AO = struct
        module Key = H
        module Val = Node.Make (H)(H)(P)(M)
        include AO.Make (AOM)(Key)(Val)
      end
      include Node.Store(Contents)(P)(M)(AO)
    end
    module Commit = struct
      module AO = struct
        module Key = H
        module Val = Commit.Make (H)(H)
        include AO.Make (AOM)(Key)(Val)
      end
      include Commit.Store(Node)(AO)
    end
    module Branch = struct
      module Key = B
      module Val = H
      include RW.Make (RWM)(Key)(Val)
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
        AOM.v config >>= fun t ->
        RWM.v config >|= fun branch ->
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
