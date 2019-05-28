module Server : sig
  module Remote : sig
    module None : sig
      val remote : Resolver.Store.remote_fn option
    end
  end

  module Make
      (S : Irmin.S) (Remote : sig
        val remote : Resolver.Store.remote_fn option
      end) :
    Irmin_graphql.Server.S
    with type repo = S.repo
     and type server = Cohttp_lwt_unix.Server.t
end
