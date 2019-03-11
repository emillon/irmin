(*
 * Copyright (c) 2016-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
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

type (_, _) eq = Refl: ('a, 'a) eq

module Witness : sig
  type 'a t
  val make : unit -> 'a t
  val eq : 'a t -> 'b t -> ('a, 'b) eq option
end = struct

  type _ equality = ..

  module type Inst = sig
    type t
    type _ equality += Eq : t equality
  end

  type 'a t = (module Inst with type t = 'a)

  let make: type a. unit -> a t = fun () ->
    let module Inst = struct
      type t = a
      type _ equality += Eq : t equality
    end
    in
    (module Inst)

  let eq: type a b. a t -> b t -> (a, b) eq option =
    fun (module A) (module B) ->
      match A.Eq with
      | B.Eq -> Some Refl
      | _    -> None

end


module Json = struct

  type decoder = {
    mutable lexemes: Jsonm.lexeme list;
    d: Jsonm.decoder;
  }

  let decoder ?encoding src = { lexemes = []; d = Jsonm.decoder ?encoding src }
  let decoder_of_lexemes lexemes = { lexemes; d = Jsonm.decoder (`String "") }
  let rewind e l = e.lexemes <- l :: e.lexemes

  let decode e =
    match e.lexemes with
    | h::t -> e.lexemes <- t; `Lexeme h
    | [] -> Jsonm.decode e.d

end

type len = [ `Int | `Int8 | `Int16 | `Int32 | `Int64 | `Fixed of int ]

type 'a pp = 'a Fmt.t
type 'a of_string = string -> ('a, [`Msg of string]) result
type 'a to_string = 'a -> string
type 'a encode_json = Jsonm.encoder -> 'a -> unit
type 'a decode_json = Json.decoder -> ('a, [`Msg of string]) result

type 'a encode_bin = toplevel:bool -> Buffer.t -> 'a -> unit
type 'a decode_bin = toplevel:bool -> string -> int -> int * 'a
type 'a size_of = toplevel:bool -> 'a -> int option

type 'a compare = 'a -> 'a -> int
type 'a equal = 'a -> 'a -> bool
type 'a hash = 'a -> int
type 'a pre_digest = 'a -> string

type 'a t =
  | Self   : 'a self -> 'a t
  | Custom : 'a custom -> 'a t
  | Map   : ('a, 'b) map -> 'b t
  | Prim   : 'a prim -> 'a t
  | List   : 'a len_v -> 'a list t
  | Array  : 'a len_v -> 'a array t
  | Tuple  : 'a tuple -> 'a t
  | Option : 'a t -> 'a option t
  | Record : 'a record -> 'a t
  | Variant: 'a variant -> 'a t

and 'a len_v = {
  len: len;
  v  : 'a t;
}

and 'a custom = {
  cwit        : [`Type of 'a t | `Witness of 'a Witness.t];
  pre_digest  : 'a pre_digest;
  pp          : 'a pp;
  of_string   : 'a of_string;
  encode_json : 'a encode_json;
  decode_json : 'a decode_json;
  encode_bin  : 'a encode_bin;
  decode_bin  : 'a decode_bin;
  hash        : 'a hash;
  size_of     : 'a size_of;
  compare     : 'a compare;
  equal       : 'a equal;
}

and ('a, 'b) map = {
  x   : 'a t;
  f   : ('a -> 'b);
  g   : ('b -> 'a);
  mwit: 'b Witness.t;
}

and 'a self = {
  mutable self: 'a t;
}

and 'a prim =
  | Unit   : unit prim
  | Bool   : bool prim
  | Char   : char prim
  | Int    : int prim
  | Int32  : int32 prim
  | Int64  : int64 prim
  | Float  : float prim
  | String : len -> string prim
  | Bytes  : len -> bytes prim

and 'a tuple =
  | Pair   : 'a t * 'b t -> ('a * 'b) tuple
  | Triple : 'a t * 'b t * 'c t -> ('a * 'b * 'c) tuple

and 'a record = {
  rwit   : 'a Witness.t;
  rname  : string;
  rfields: 'a fields_and_constr;
}

and 'a fields_and_constr =
  | Fields: ('a, 'b) fields * 'b -> 'a fields_and_constr

and ('a, 'b) fields =
  | F0: ('a, 'a) fields
  | F1: ('a, 'b) field * ('a, 'c) fields -> ('a, 'b -> 'c) fields

and ('a, 'b) field = {
  fname: string;
  ftype: 'b t;
  fget : 'a -> 'b;
}

and 'a variant = {
  vwit  : 'a Witness.t;
  vname : string;
  vcases: 'a a_case array;
  vget  : 'a -> 'a case_v;
}

and 'a a_case =
  | C0: 'a case0 -> 'a a_case
  | C1: ('a, 'b) case1 -> 'a a_case

and 'a case_v =
  | CV0: 'a case0 -> 'a case_v
  | CV1: ('a, 'b) case1 * 'b -> 'a case_v

and 'a case0 = {
  ctag0 : int;
  cname0: string;
  c0    : 'a;
}

and ('a, 'b) case1 = {
  ctag1 : int;
  cname1: string;
  ctype1: 'b t;
  c1    : 'b -> 'a;
}

type _ a_field = Field: ('a, 'b) field -> 'a a_field

let rec pp_ty: type a. a t Fmt.t = fun ppf -> function
  | Self s -> Fmt.pf ppf "@[Self (%a@)]" pp_ty s.self
  | Custom c -> Fmt. pf ppf "@[Custom (%a)@]" pp_custom c
  | Map m -> Fmt.pf ppf "@[Map (%a)]" pp_ty m.x
  | Prim p -> Fmt.pf ppf "@[Prim %a@]" pp_prim p
  | List l -> Fmt.pf ppf"@[List%a (%a)@]" pp_len l.len pp_ty l.v
  | Array a -> Fmt.pf ppf "@[Array%a (%a)@]" pp_len a.len pp_ty a.v
  | Tuple (Pair (a, b)) -> Fmt.pf ppf "@[Pair (%a, %a)@]" pp_ty a pp_ty b
  | Tuple (Triple (a, b, c)) ->
    Fmt.pf ppf "@[Triple (%a, %a, %a)@]" pp_ty a pp_ty b pp_ty c
  | Option t -> Fmt.pf ppf "@[Option (%a)@]" pp_ty t
  | Record _ -> Fmt.pf ppf "@[Record@]"
  | Variant _ -> Fmt.pf ppf "@[Variant@]"

and pp_custom: type a. a custom Fmt.t = fun ppf c -> match c.cwit with
  | `Type t -> pp_ty ppf t
  | `Witness _ -> Fmt.string ppf "-"

and pp_prim: type a. a prim Fmt.t = fun ppf -> function
  | Unit -> Fmt.string ppf "Unit"
  | Bool -> Fmt.string ppf "Bool"
  | Char -> Fmt.string ppf "Char"
  | Int    -> Fmt.string ppf "Int"
  | Int32  -> Fmt.string ppf "Int32"
  | Int64  -> Fmt.string ppf "Int64"
  | Float  -> Fmt.string ppf "Float"
  | String n -> Fmt.pf ppf "String%a" pp_len n
  | Bytes n -> Fmt.pf ppf "Bytes%a" pp_len n

and pp_len: len Fmt.t = fun ppf -> function
  | `Int8    -> Fmt.string ppf ":8"
  | `Int64   -> Fmt.string ppf ":64"
  | `Int16   -> Fmt.string ppf ":16"
  | `Fixed n -> Fmt.pf ppf ":<%d>" n
  | `Int     -> ()
  | `Int32   -> Fmt.pf ppf ":32"

module Refl = struct

  let prim: type a b. a prim -> b prim -> (a, b) eq option = fun a b ->
    match a, b with
    | Unit  , Unit   -> Some Refl
    | Bool  , Bool   -> Some Refl
    | Char  , Char   -> Some Refl
    | Int   , Int    -> Some Refl
    | Int32 , Int32  -> Some Refl
    | Int64 , Int64  -> Some Refl
    | Float , Float   -> Some Refl
    | String _  , String _  -> Some Refl
    | Bytes _   , Bytes _   -> Some Refl
    | _ -> None

  let rec t: type a b. a t -> b t -> (a, b) eq option = fun a b ->
    match a, b with
    | Self a, _ -> t a.self b
    | _, Self b -> t a b.self
    | Map a, Map b -> Witness.eq a.mwit b.mwit
    | Custom a, Custom b -> custom a b
    | Prim a, Prim b -> prim a b
    | Array a, Array b ->
      (match t a.v b.v with Some Refl -> Some Refl | None -> None)
    | List a, List b ->
      (match t a.v b.v with Some Refl -> Some Refl | None -> None)
    | Tuple a, Tuple b -> tuple a b
    | Option a, Option b ->
      (match t a b with Some Refl -> Some Refl | None -> None)
    | Record a, Record b   -> Witness.eq a.rwit b.rwit
    | Variant a, Variant b -> Witness.eq a.vwit b.vwit
    | _ -> None

  and custom: type a b. a custom -> b custom -> (a, b) eq option = fun a b ->
    match a.cwit, b.cwit with
    | `Witness a, `Witness b -> Witness.eq a b
    | `Type a, `Type b -> t a b
    | _ -> None

  and tuple: type a b. a tuple -> b tuple -> (a, b) eq option = fun a b ->
    match a, b with
    | Pair (a0, a1), Pair (b0, b1) ->
      (match t a0 b0, t a1 b1 with
       | Some Refl, Some Refl -> Some Refl
       | _ -> None)
    | Triple (a0, a1, a2), Triple (b0, b1, b2) ->
      (match t a0 b0, t a1 b1, t a2 b2 with
       | Some Refl, Some Refl, Some Refl -> Some Refl
       | _ -> None)
    | _ -> None

end

let unit = Prim Unit
let bool = Prim Bool
let char = Prim Char
let int = Prim Int
let int32 = Prim Int32
let int64 = Prim Int64
let float = Prim Float
let string = Prim (String `Int)
let bytes = Prim (Bytes `Int)
let string_of n = Prim (String n)
let bytes_of n = Prim (Bytes n)

let list ?(len=`Int) v = List { v; len }
let array ?(len=`Int) v = Array { v; len }
let pair a b = Tuple (Pair (a, b))
let triple a b c = Tuple (Triple (a, b, c))
let option a = Option a

let v ~cli ~json ~bin ~equal ~compare ~hash ~pre_digest =
  let pp, of_string = cli in
  let encode_json, decode_json = json in
  let encode_bin, decode_bin, size_of = bin in
  Custom {
    cwit = `Witness (Witness.make ());
    pp; of_string; pre_digest;
    encode_json; decode_json;
    encode_bin; decode_bin; size_of;
    compare; equal; hash
  }

(* fix points *)

let mu: type a. (a t -> a t) -> a t = fun f ->
  let rec fake_x = { self = Self fake_x } in
  let real_x = f (Self fake_x) in
  fake_x.self <- real_x;
  real_x

let mu2: type a b. (a t -> b t -> a t * b t) -> a t * b t = fun f ->
  let rec fake_x = { self = Self fake_x } in
  let rec fake_y = { self = Self fake_y } in
  let real_x, real_y = f (Self fake_x) (Self fake_y) in
  fake_x.self <- real_x;
  fake_y.self <- real_y;
  real_x, real_y

(* records *)

type ('a, 'b, 'c) open_record =
  ('a, 'c) fields -> string * 'b * ('a, 'b) fields

let field fname ftype fget = { fname; ftype; fget }

let record: string -> 'b -> ('a, 'b, 'b) open_record =
  fun n c fs -> n, c, fs

let app: type a b c d.
  (a, b, c -> d) open_record -> (a, c) field -> (a, b, d) open_record
  = fun r f fs ->
    let n, c, fs = r (F1 (f, fs)) in
    n, c, fs

let sealr: type a b. (a, b, a) open_record -> a t =
  fun r ->
    let rname, c, fs = r F0 in
    let rwit = Witness.make () in
    Record { rwit; rname; rfields = Fields (fs, c) }

let (|+) = app

(* variants *)

type 'a case_p = 'a case_v

type ('a, 'b) case = int -> ('a a_case * 'b)

let case0 cname0 c0 ctag0 =
  let c = { ctag0; cname0; c0 } in
  C0 c, CV0 c

let case1 cname1 ctype1 c1 ctag1 =
  let c = { ctag1; cname1; ctype1; c1 } in
  C1 c, fun v -> CV1 (c, v)

type ('a, 'b, 'c) open_variant = 'a a_case list -> string * 'c * 'a a_case list

let variant n c vs = n, c, vs

let app v c cs =
  let n, fc, cs = v cs in
  let c, f = c (List.length cs) in
  n, fc f, (c :: cs)

let sealv v =
  let vname, vget, vcases = v [] in
  let vwit = Witness.make () in
  let vcases = Array.of_list (List.rev vcases) in
  Variant { vwit; vname; vcases ; vget }

let (|~) = app

let enum vname l =
  let vwit = Witness.make () in
  let _, vcases, mk =
    List.fold_left (fun (ctag0, cases, mk) (n, v) ->
        let c = { ctag0; cname0 = n; c0 = v } in
        ctag0+1, (C0 c :: cases), (v, CV0 c) :: mk
      ) (0, [], []) l
  in
  let vcases = Array.of_list (List.rev vcases) in
  Variant { vwit; vname; vcases; vget = fun x -> List.assq x mk }

let rec fields_aux: type a b. (a, b) fields -> a a_field list = function
  | F0        -> []
  | F1 (h, t) -> Field h :: fields_aux t

let fields r = match r.rfields with
  | Fields (f, _) -> fields_aux f

let result a b =
  variant "result" (fun ok error -> function
      | Ok x    -> ok x
      | Error x -> error x)
  |~ case1 "ok"    a (fun a -> Ok a)
  |~ case1 "error" b (fun b -> Error b)
  |> sealv

module Equal = struct

  let unit _ _ = true
  let bool (x:bool) (y:bool) = x = y
  let char (x:char) (y:char) = x = y
  let int (x:int) (y:int) = x = y
  let int32 (x:int32) (y:int32) = x = y
  let int64 (x:int64) (y:int64) = x = y
  let string x y = x == y || String.equal x y
  let bytes x y = x == y || Bytes.equal x y

  (* NOTE: equality is ill-defined on float *)
  let float (x:float) (y:float) =  x = y

  let list e x y =
    x == y || (List.length x = List.length y && List.for_all2 e x y)

  let array e x y =
    x == y ||
    (Array.length x = Array.length y &&
     let rec aux = function
       | -1 -> true
       | i  -> e x.(i) y.(i) && aux (i-1)
     in aux (Array.length x - 1))

  let pair ex ey (x1, y1 as a) (x2, y2 as b) =
    a == b || (ex x1 x2 && ey y1 y2)

  let triple ex ey ez (x1, y1, z1 as a) (x2, y2, z2 as b) =
    a == b || (ex x1 x2 && ey y1 y2 && ez z1 z2)

  let option e x y =
    x == y ||
    match x, y with
    | None  , None   -> true
    | Some x, Some y -> e x y
    | _ -> false

  let rec t: type a. a t -> a equal = function
    | Self s    -> t s.self
    | Custom c  -> c.equal
    | Map b     -> map b
    | Prim p    -> prim p
    | List l    -> list (t l.v)
    | Array a   -> array (t a.v)
    | Tuple t   -> tuple t
    | Option x  -> option (t x)
    | Record r  -> record r
    | Variant v -> variant v

  and tuple: type a. a tuple -> a equal = function
    | Pair (a, b)      -> pair (t a) (t b)
    | Triple (a, b, c) -> triple (t a) (t b) (t c)

  and map: type a b. (a, b) map -> b equal =
    fun { x; g; _ } u v -> t x (g u) (g v)

  and prim: type a. a prim -> a equal = function
    | Unit   -> unit
    | Bool   -> bool
    | Char   -> char
    | Int    -> int
    | Int32  -> int32
    | Int64  -> int64
    | Float  -> float
    | String _  -> string
    | Bytes _   -> bytes

  and record: type a. a record -> a equal = fun r x y ->
    List.for_all (function Field f -> field f x y) (fields r)

  and field: type a  b. (a, b) field -> a equal = fun f x y ->
    t f.ftype (f.fget x) (f.fget y)

  and variant: type a. a variant -> a equal = fun v x y ->
    case_v (v.vget x) (v.vget y)

  and case_v: type a. a case_v equal = fun x y ->
    match x, y with
    | CV0 x      , CV0 y       -> int x.ctag0 y.ctag0
    | CV1 (x, vx), CV1 (y, vy) -> int x.ctag1 y.ctag1 &&
                                  eq (x.ctype1, vx) (y.ctype1, vy)
    | _ -> false

  and eq: type a b. (a t * a) -> (b t * b) -> bool = fun (tx, x) (ty, y) ->
    match Refl.t tx ty with
    | Some Refl -> t tx x y
    | None      -> assert false (* this should never happen *)

end

let equal = Equal.t

module Compare = struct

  let unit (_:unit) (_:unit) = 0
  let bool (x:bool) (y:bool) = Pervasives.compare x y
  let char = Char.compare
  let int (x:int) (y:int) = Pervasives.compare x y
  let int32 = Int32.compare
  let int64 = Int64.compare
  let float (x:float) (y:float) = Pervasives.compare x y
  let string x y = if x == y then 0 else String.compare x y
  let bytes x y = if x == y then 0 else Bytes.compare x y

  let list c x y =
    if x == y then 0 else
      let rec aux x y = match x, y with
        | [], [] -> 0
        | [], _  -> -1
        | _ , [] -> 1
        | xx::x,yy::y -> match c xx yy with
          | 0 -> aux x y
          | i -> i
      in
      aux x y

  let array c x y =
    if x == y then 0 else
      let lenx = Array.length x in
      let leny = Array.length y in
      if lenx > leny then 1
      else if lenx < leny then -1
      else
        let rec aux i = match c x.(i) y.(i) with
          | 0 when i+1 = lenx -> 0
          | 0 -> aux (i+1)
          | i -> i
        in
        aux 0

  let pair cx cy (x1, y1 as a) (x2, y2 as b) =
    if a == b then 0 else
      match cx x1 x2 with
      | 0 -> cy y1 y2
      | i -> i

  let triple cx cy cz (x1, y1, z1 as a) (x2, y2, z2 as b) =
    if a == b then 0 else
      match cx x1 x2 with
      | 0 -> pair cy cz (y1, z1) (y2, z2)
      | i -> i

  let option c x y =
    if x == y then 0 else
      match x, y with
      | None  , None   -> 0
      | Some _, None   -> 1
      | None  , Some _ -> -1
      | Some x, Some y -> c x y

  let rec t: type a. a t -> a compare = function
    | Self s    -> t s.self
    | Custom b  -> b.compare
    | Map b     -> map b
    | Prim p    -> prim p
    | List l    -> list (t l.v)
    | Array a   -> array (t a.v)
    | Tuple t   -> tuple t
    | Option x  -> option (t x)
    | Record r  -> record r
    | Variant v -> variant v

  and tuple: type a. a tuple -> a compare = function
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b compare =
    fun { x; g; _ } u v -> t x (g u) (g v)

  and prim: type a. a prim -> a compare = function
    | Unit   -> unit
    | Bool   -> bool
    | Char   -> char
    | Int    -> int
    | Int32  -> int32
    | Int64  -> int64
    | Float  -> float
    | String _  -> string
    | Bytes _   -> bytes

  and record: type a. a record -> a compare = fun r x y ->
    let rec aux = function
      | []           -> 0
      | Field f :: t -> match field f x y with  0 -> aux t | i -> i
    in
    aux (fields r)

  and field: type a  b. (a, b) field -> a compare = fun f x y ->
    t f.ftype (f.fget x) (f.fget y)

  and variant: type a. a variant -> a compare = fun v x y ->
    case_v (v.vget x) (v.vget y)

  and case_v: type a. a case_v compare = fun x y ->
    match x, y with
    | CV0 x      , CV0 y       -> int x.ctag0 y.ctag0
    | CV0 x      , CV1 (y, _)  -> int x.ctag0 y.ctag1
    | CV1 (x, _) , CV0 y       -> int x.ctag1 y.ctag0
    | CV1 (x, vx), CV1 (y, vy) ->
      match int x.ctag1 y.ctag1 with
      | 0 -> compare (x.ctype1, vx) (y.ctype1, vy)
      | i -> i

  and compare: type a b. (a t * a) -> (b t * b) -> int = fun (tx, x) (ty, y) ->
    match Refl.t tx ty with
    | Some Refl -> t tx x y
    | None      -> assert false (* this should never happen *)

end

let compare = Compare.t


exception Not_utf8

let is_valid_utf8 str =
  try
    Uutf.String.fold_utf_8 (fun _ _ -> function
        | `Malformed _ -> raise Not_utf8
        | _ -> ()
      ) () str;
    true
with Not_utf8 -> false

module Encode_json = struct

  let lexeme e l = ignore (Jsonm.encode e (`Lexeme l))

  let unit e () = lexeme e `Null

  let base64 e s =
    let x = Base64.encode_exn s in
    lexeme e `Os;
    lexeme e (`Name "base64");
    lexeme e (`String x);
    lexeme e `Oe

  let string e s =
    if is_valid_utf8 s then
      lexeme e (`String s)
    else
      base64 e s

  let bytes e b =
    let s = Bytes.unsafe_to_string b in
    string e s

  let char e c =
    let s = String.make 1 c in
    string e s

  let float e f = lexeme e (`Float f)
  let int e i = float e (float_of_int i)
  let int32 e i = float e (Int32.to_float i)
  let int64 e i = float e (Int64.to_float i)
  let bool e = function false -> float e 0. | _ -> float e 1.

  let list l e x =
    lexeme e `As;
    List.iter (l e) x;
    lexeme e `Ae

  let array l e x =
    lexeme e `As;
    Array.iter (l e) x;
    lexeme e `Ae

  let pair a b e (x, y) =
    lexeme e `As;
    a e x;
    b e y;
    lexeme e `Ae

  let triple a b c e (x, y, z) =
    lexeme e `As;
    a e x;
    b e y;
    c e z;
    lexeme e `Ae

  let option o e = function
    | None   -> lexeme e `Null
    | Some x -> o e x

  let rec t: type a. a t -> a encode_json = function
    | Self s    -> t s.self
    | Custom c  -> c.encode_json
    | Map b     -> map b
    | Prim t    -> prim t
    | List l    -> list (t l.v)
    | Array a   -> array (t a.v)
    | Tuple t   -> tuple t
    | Option x  -> option (t x)
    | Record r  -> record r
    | Variant v -> variant v

  and tuple: type a. a tuple -> a encode_json = function
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b encode_json =
    fun { x; g; _ } e u -> t x e (g u)

  and prim: type a. a prim -> a encode_json = function
    | Unit   -> unit
    | Bool   -> bool
    | Char   -> char
    | Int    -> int
    | Int32  -> int32
    | Int64  -> int64
    | Float  -> float
    | String _  -> string
    | Bytes _   -> bytes

  and record: type a. a record -> a encode_json = fun r e x ->
    let fields = fields r in
    lexeme e `Os;
    List.iter (fun (Field f) ->
        match f.ftype, f.fget x with
        | Option _, None   -> ()
        | Option o, Some x -> lexeme e (`Name f.fname); t o e x
        | tx      , x      -> lexeme e (`Name f.fname); t tx e x
      ) fields;
    lexeme e `Oe

  and variant: type a. a variant -> a encode_json = fun v e x ->
    case_v e (v.vget x)

  and case_v: type a. a case_v encode_json = fun e c ->
    match c with
    | CV0 c     -> string e c.cname0
    | CV1 (c,v) ->
      lexeme e `Os;
      lexeme e (`Name c.cname1);
      t c.ctype1 e v;
      lexeme e `Oe

end

let encode_json = Encode_json.t

let pp_json ?minify t ppf x =
  let buf = Buffer.create 42 in
  let e = Jsonm.encoder ?minify (`Buffer buf) in
  encode_json t e x;
  ignore (Jsonm.encode e `End);
  Fmt.string ppf (Buffer.contents buf)

let to_json_string ?minify t = Fmt.to_to_string (pp_json ?minify t)

module Decode_json = struct

  let lexeme e = match Json.decode e with
    | `Lexeme e     -> Ok e
    | `Error e      -> Error (`Msg (Fmt.to_to_string Jsonm.pp_error e))
    | `End | `Await -> assert false

  let (>>=) l f = match l with
    | Error _ as e -> e
    | Ok l -> f l

  let (>|=) l f = match l with
    | Ok l -> Ok (f l)
    | Error _ as e -> e

  let error e got expected =
    let _, (l, c) = Jsonm.decoded_range e.Json.d in
    Error (`Msg (Fmt.strf
                   "line %d, character %d:\nFound lexeme %a, but \
                    lexeme %s was expected" l c Jsonm.pp_lexeme got expected))

  let expect_lexeme e expected =
    lexeme e >>= fun got ->
    if expected = got then Ok ()
    else error e got (Fmt.to_to_string Jsonm.pp_lexeme expected)

  (* read all lexemes until the end of the next well-formed value *)
  let value e =
    let lexemes = ref [] in
    let objs = ref 0 in
    let arrs = ref 0 in
    let rec aux () =
      lexeme e >>= fun l ->
      lexemes := l :: !lexemes;
      let () = match l with
        | `Os -> incr objs
        | `As -> incr arrs
        | `Oe -> decr objs
        | `Ae -> decr arrs
        | `Name _
        | `Null
        | `Bool _
        | `String _
        | `Float _ -> ()
      in
      if !objs > 0 || !arrs > 0 then aux ()
      else Ok ()
    in
    aux () >|= fun () ->
    List.rev !lexemes

  let unit e = expect_lexeme e `Null

  let get_base64_value e =
    match lexeme e with
    | Ok (`Name "base64") ->
        (match lexeme e with
        | Ok (`String b) ->
            (match expect_lexeme e `Oe with
            | Ok () ->
              Ok (Base64.decode_exn b)
            | Error e -> Error e)
        | Ok l -> error e l "Bad base64 encoded character"
        | Error e -> Error e)
    | Ok l -> error e l "Invalid base64 object"
    | Error e -> Error e

  let string e =
    lexeme e >>= function
    | `String s -> Ok s
    | `Os -> get_base64_value e
    | l         -> error e l "`String"

  let bytes e =
    lexeme e >>= function
    | `String s -> Ok (Bytes.unsafe_of_string s)
    | `Os ->
        (match get_base64_value e with
        | Ok s -> Ok (Bytes.unsafe_of_string s)
        | Error e -> Error e)
    | l         -> error e l "`String"

  let float e =
    lexeme e >>= function
    | `Float f -> Ok f
    | l        -> error e l "`Float"

  let char e =
    lexeme e >>= function
    | `String s when String.length s = 1 -> Ok (String.get s 0)
    | `Os ->
      (match get_base64_value e with
      | Ok s ->
          Ok (String.get s 0)
      | Error x -> Error x)
    | l -> error e l "`String[0]"

  let int32 e = float e >|= Int32.of_float
  let int64 e = float e >|= Int64.of_float
  let int e   = float e >|= int_of_float
  let bool e  = int e >|= function 0 -> false | _ -> true

  let list l e =
    expect_lexeme e `As >>= fun () ->
    let rec aux acc =
      lexeme e >>= function
      | `Ae -> Ok (List.rev acc)
      | lex ->
        Json.rewind e lex;
        l e >>= fun v ->
        aux (v :: acc)
    in
    aux []

  let array l e = list l e >|= Array.of_list

  let pair a b e =
    expect_lexeme e `As >>= fun () ->
    a e >>= fun x ->
    b e >>= fun y ->
    expect_lexeme e `Ae >|= fun () ->
    x, y

  let triple a b c e =
    expect_lexeme e `As >>= fun () ->
    a e >>= fun x ->
    b e >>= fun y ->
    c e >>= fun z ->
    expect_lexeme e `Ae >|= fun () ->
    x, y, z

  let option o e =
    lexeme e >>= function
    | `Null -> Ok None
    | lex   ->
      Json.rewind e lex;
      o e >|= fun v -> Some v

  let rec t: type a. a t -> a decode_json = function
    | Self s    -> t s.self
    | Custom c  -> c.decode_json
    | Map b     -> map b
    | Prim t    -> prim t
    | List l    -> list (t l.v)
    | Array a   -> array (t a.v)
    | Tuple t   -> tuple t
    | Option x  -> option (t x)
    | Record r  -> record r
    | Variant v -> variant v

  and tuple: type a. a tuple -> a decode_json = function
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b decode_json =
    fun { x; f; _ } e -> t x e >|= f

  and prim: type a. a prim -> a decode_json = function
    | Unit   -> unit
    | Bool   -> bool
    | Char   -> char
    | Int    -> int
    | Int32  -> int32
    | Int64  -> int64
    | Float  -> float
    | String _  -> string
    | Bytes _   -> bytes

  and record: type a. a record -> a decode_json = fun r e ->
    expect_lexeme e `Os >>= fun () ->
    let rec soup acc =
      lexeme e >>= function
      | `Name n ->
        value e >>= fun s ->
        soup ((n, s) :: acc)
      | `Oe -> Ok acc
      | l   -> error e l "`Record-contents"
    in
    soup [] >>= fun soup ->
    let rec aux: type a b. (a, b) fields -> b -> (a, [`Msg of string]) result =
      fun f c -> match f with
      | F0        -> Ok c
      | F1 (h, f) ->
        let v =
          try
            let s = List.assoc h.fname soup in
            let e = Json.decoder_of_lexemes s in
            t h.ftype e
          with Not_found ->
          match h.ftype with
          | Option _ -> Ok None
          | _        ->
            Error (`Msg (Fmt.strf "missing value for %s.%s" r.rname h.fname))
        in
        match v with
        | Ok v         -> aux f (c v)
        | Error _ as e -> e
    in
    let Fields (f, c) = r.rfields in
    aux f c

  and variant: type a. a variant -> a decode_json = fun v e ->
    lexeme e >>= function
    | `String s -> case0 s v e
    | `Os       -> case1 v e
    | l         -> error e l "(`String | `Os)"

  and case0: type a. string -> a variant -> a decode_json = fun s v _e ->
    let rec aux i = match v.vcases.(i) with
      | C0 c when String.compare c.cname0 s = 0 -> Ok c.c0
      | _ ->
        if i < Array.length v.vcases
        then aux (i+1)
        else Error (`Msg "variant")
    in
    aux 0

  and case1: type a. a variant -> a decode_json = fun v e ->
    lexeme e >>= function
    | `Name s ->
      let rec aux i = match v.vcases.(i) with
        | C1 c when String.compare c.cname1 s = 0 -> t c.ctype1 e >|= c.c1
        | _ ->
          if i < Array.length v.vcases
          then aux (i+1)
          else Error (`Msg "variant")
      in
      aux 0 >>= fun c ->
      expect_lexeme e `Oe >|= fun () ->
      c
    | l -> error e l "`Name"

end

let decode_json x d = Decode_json.(t x @@ { Json.d; lexemes = [] })
let decode_json_lexemes x ls = Decode_json.(t x @@ Json.decoder_of_lexemes ls)
let of_json_string x s = Decode_json.(t x @@ Json.decoder (`String s))

module Size_of = struct

  let (>>=) x f = match x with
    | None   -> None
    | Some x -> f x

  let (>|=) x f = match x with
    | None   -> None
    | Some x -> Some (f x)

  let int n =
    let rec aux len n =
      if n >= 0 && n < 128 then len
      else aux (len+1) (n lsr 7)
    in
    Some (aux 1 n)

  let len n = function
    | `Int     -> int n
    | `Int8    -> Some 1
    | `Int16   -> Some 2
    | `Int32   -> Some 4
    | `Int64   -> Some 8
    | `Fixed _ -> Some 0

  let unit () = Some 0
  let char (_:char) = Some 1
  let int32 (_:int32) = Some 4
  let int64 (_:int64) = Some 8
  let bool (_:bool) = Some 1
  let float (_:float) = Some 8 (* NOTE: we consider 'double' here *)

  let string ~toplevel n s =
    let s = String.length s in
    if toplevel then Some s else len s n >|= fun i -> i + s

  let bytes ~toplevel n s =
    let s = Bytes.length s in
    if toplevel then Some s else len s n >|= fun i -> i + s

  let list l n x =
    let init = len (List.length x) n in
    List.fold_left (fun acc x ->
        acc >>= fun acc ->
        l x >|= fun l ->
        acc + l
      ) init x

  let array l n x =
    let init = len (Array.length x) n in
    Array.fold_left (fun acc x ->
        acc >>= fun acc ->
        l x >|= fun l ->
        acc + l
      ) init x

  let pair a b (x, y) =
    a x >>= fun a ->
    b y >|= fun b ->
    a + b

  let triple a b c (x, y, z) =
    a x >>= fun a ->
    b y >>= fun b ->
    c z >|= fun c ->
    a + b+ c

  let option o = function
  | None   -> char '\000'
  | Some x ->
    char '\000' >>= fun i ->
    o x >|= fun o ->
    i + o

  let rec t: type a. a t -> a size_of = fun ty ~toplevel -> match ty with
  | Self s    -> t ~toplevel s.self
  | Custom c  -> c.size_of ~toplevel
  | Map b     -> map ~toplevel b
  | Prim t    -> prim ~toplevel t
  | List l    -> list (t ~toplevel:false l.v) l.len
  | Array a   -> array (t ~toplevel:false a.v) a.len
  | Tuple t   -> tuple ~toplevel t
  | Option x  -> option (t ~toplevel:false x)
  | Record r  -> record ~toplevel r
  | Variant v -> variant ~toplevel v

  and tuple: type a. a tuple -> a size_of = fun ty ~toplevel:_ ->
    let t = t ~toplevel:false in
    match ty with
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b size_of =
    fun { x; g; _ } ~toplevel u -> t ~toplevel x (g u)

  and prim: type a. a prim -> a size_of = fun ty ~toplevel -> match ty with
    | Unit      -> unit
    | Bool      -> bool
    | Char      -> char
    | Int       -> int
    | Int32     -> int32
    | Int64     -> int64
    | Float     -> float
    | String n  -> string ~toplevel n
    | Bytes  n  -> bytes ~toplevel n

  and record: type a. a record -> a size_of = fun r ~toplevel:_ x ->
    let fields = fields r in
    let s =
      List.fold_left (fun acc (Field f) ->
          acc >>= fun acc ->
          field ~toplevel:false f x >|= fun f ->
          acc + f
        ) (Some 0) fields
    in
    s

  and field: type a b. (a, b) field -> a size_of = fun f  ~toplevel x ->
    t ~toplevel f.ftype (f.fget x)

  and variant: type a. a variant -> a size_of = fun v ~toplevel x ->
    match v.vget x with
    | CV0 _       -> int (Array.length v.vcases)
    | CV1 (x, vx) ->
      int (Array.length v.vcases) >>= fun v ->
      t ~toplevel x.ctype1 vx >|= fun x ->
      v + x

end

let size_of = Size_of.t

module B = struct

  external get_16 : string -> int -> int = "%caml_string_get16"
  external get_32 : string -> int -> int32 = "%caml_string_get32"
  external get_64 : string -> int -> int64 = "%caml_string_get64"

  external set_16 : Bytes.t -> int -> int -> unit = "%caml_string_set16u"
  external set_32 : Bytes.t -> int -> int32 -> unit = "%caml_string_set32u"
  external set_64 : Bytes.t -> int -> int64 -> unit = "%caml_string_set64u"

  external swap16 : int -> int = "%bswap16"
  external swap32 : int32 -> int32 = "%bswap_int32"
  external swap64 : int64 -> int64 = "%bswap_int64"

  let get_uint16 s off =
    if not Sys.big_endian then swap16 (get_16 s off) else get_16 s off

  let get_uint32 s off =
    if not Sys.big_endian then swap32 (get_32 s off) else get_32 s off

  let get_uint64 s off =
    if not Sys.big_endian then swap64 (get_64 s off) else get_64 s off

  let set_uint16 s off v =
    if not Sys.big_endian then set_16 s off (swap16 v) else set_16 s off v

  let set_uint32 s off v =
    if not Sys.big_endian then set_32 s off (swap32 v) else set_32 s off v

  let set_uint64 s off v =
    if not Sys.big_endian then set_64 s off (swap64 v) else set_64 s off v

end

module Encode_bin = struct

  let unit _buf () = ()
  let char buf c = Buffer.add_char buf c
  let int8 buf i = Buffer.add_char buf (Char.chr i)

  let int16 buf i =
    let b = Bytes.create 2 in
    B.set_uint16 b 0 i;
    Buffer.add_bytes buf b

  let int32 buf i =
    let b = Bytes.create 4 in
    B.set_uint32 b 0 i;
    Buffer.add_bytes buf b

  let int64 buf i =
    let b = Bytes.create 8 in
    B.set_uint64 b 0 i;
    Buffer.add_bytes buf b

  let float buf f = int64 buf (Int64.bits_of_float f)
  let bool buf b = char buf (if b then '\255' else '\000')

  let int buf i =
    let rec aux n =
      if n >= 0 && n < 128 then
        int8 buf n
      else
        let out = 128 + (n land 127) in
        int8 buf out;
        aux (n lsr 7)
    in
    aux i

  let len n buf i = match n with
    | `Int     -> int buf i
    | `Int8    -> int8 buf i
    | `Int16   -> int16 buf i
    | `Int32   -> int32 buf (Int32.of_int i)
    | `Int64   -> int64 buf (Int64.of_int i)
    | `Fixed _ -> ()

  let string ~toplevel n buf s =
    if toplevel then
      Buffer.add_string buf s
    else (
      let k = String.length s in
      len n buf k;
      Buffer.add_string buf s;
    )

  let bytes ~toplevel n buf s =
    if toplevel then
      Buffer.add_bytes buf s
    else (
      let k = Bytes.length s in
      len n buf k;
      Buffer.add_bytes buf s;
    )

  let list l n buf x =
    len n buf (List.length x);
    List.iter (fun e -> l buf e) x

  let array l n buf x =
    len n buf (Array.length x);
    Array.iter (fun e -> l buf e) x

  let pair a b buf (x, y) =
    a buf x;
    b buf y

  let triple a b c buf (x, y, z) =
    a buf x;
    b buf y;
    c buf z

  let option o buf = function
    | None   -> char buf '\000'
    | Some x -> char buf '\255'; o buf x

  let rec t: type a. a t -> a encode_bin = fun ty ~toplevel -> match ty with
    | Self s    -> t ~toplevel s.self
    | Custom c  -> c.encode_bin ~toplevel
    | Map b     -> map ~toplevel b
    | Prim t    -> prim ~toplevel t
    | List l    -> list (t ~toplevel:false l.v) l.len
    | Array a   -> array (t ~toplevel:false a.v) a.len
    | Tuple t   -> tuple ~toplevel t
    | Option x  -> option (t ~toplevel:false x)
    | Record r  -> record ~toplevel r
    | Variant v -> variant ~toplevel v

  and tuple: type a. a tuple -> a encode_bin = fun ty ~toplevel:_ ->
    let t = t ~toplevel:false in
    match ty with
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b encode_bin =
    fun { x; g; _ } ~toplevel buf u -> t ~toplevel x buf (g u)

  and prim: type a. a prim -> a encode_bin = fun ty ~toplevel -> match ty with
    | Unit     -> unit
    | Bool     -> bool
    | Char     -> char
    | Int      -> int
    | Int32    -> int32
    | Int64    -> int64
    | Float    -> float
    | String n -> string ~toplevel n
    | Bytes n  -> bytes ~toplevel n

  and record: type a. a record -> a encode_bin = fun r ~toplevel:_ buf x ->
    let fields = fields r in
    List.iter (fun (Field f) ->
        t ~toplevel:false f.ftype buf (f.fget x)
      ) fields

  and variant: type a. a variant -> a encode_bin = fun v ~toplevel buf x ->
    case_v ~toplevel buf (v.vget x)

  and case_v: type a. a case_v encode_bin = fun ~toplevel buf c ->
    match c with
    | CV0 c     -> int buf c.ctag0
    | CV1 (c, v) ->
      int buf c.ctag1;
      t ~toplevel c.ctype1 buf v

end

let encode_bin = Encode_bin.t

let pp t =
  let rec aux: type a. a t -> a pp = fun t ppf x ->
    match t with
    | Self   s -> aux s.self ppf x
    | Custom c -> c.pp ppf x
    | Map l    -> map l ppf x
    | Prim p   -> prim p ppf x
    | _        -> pp_json t ppf x

  and map: type a b. (a, b) map -> b pp = fun l ppf x ->
    aux l.x ppf (l.g x)

  and prim: type a. a prim -> a pp = fun t ppf x ->
    match t with
    | Unit     -> ()
    | Bool     -> Fmt.bool ppf x
    | Char     -> Fmt.char ppf x
    | Int      -> Fmt.int ppf x
    | Int32    -> Fmt.int32 ppf x
    | Int64    -> Fmt.int64 ppf x
    | Float    -> Fmt.float ppf x
    | String _ -> Fmt.string ppf x
    | Bytes _  -> Fmt.string ppf (Bytes.unsafe_to_string x)
  in
  aux t

let to_bin size_of encode_bin x =
  let len = match size_of ~toplevel:true x with
    | None   -> 1024
    | Some s -> s
  in
  let buf = Buffer.create len in
  encode_bin ~toplevel:true buf x;
  Buffer.contents buf

let to_bin_string t x =
  let rec aux: type a. a t -> a -> string = fun t x ->
    Fmt.epr "XXX to_bin_string %a\n%!" pp_ty t;
    match t with
    | Self s           -> aux s.self x
    | Map m            -> aux m.x (m.g x)
    | Prim (String _)  -> x
    | Prim (Bytes _)   -> Bytes.to_string x
    | Custom c         -> to_bin c.size_of c.encode_bin x
    | _                -> to_bin (size_of t) (encode_bin t) x
  in
  let y = aux t x in
  Fmt.epr "XXX %a -> %S\n%!" (pp t) x y;
  y

module Decode_bin = struct

  let (>|=) (ofs, x) f = ofs, f x
  let (>>=) (ofs, x) f = f (ofs, x)
  let ok ofs x  = (ofs, x)

  type 'a res = int * 'a

  let unit _ ofs = ok ofs ()
  let char buf ofs = ok (ofs+1) (String.get buf ofs)
  let int8 buf ofs = char buf ofs >|= Char.code
  let int16 buf ofs = ok (ofs+2) (B.get_uint16 buf ofs)
  let int32 buf ofs = ok (ofs+4) (B.get_uint32 buf ofs)
  let int64 buf ofs = ok (ofs+8) (B.get_uint64 buf ofs)
  let bool buf ofs = char buf ofs >|= function '\000' -> false | _ -> true
  let float buf ofs = int64 buf ofs >|= Int64.float_of_bits

  let int buf ofs =
    let rec aux n p ofs =
      int8 buf ofs >>= fun (ofs, i) ->
      let n = n + ((i land 127) lsl (p*7)) in
      if i >= 0 && i < 128 then (ofs, n)
      else aux n (p+1) ofs
    in
    aux 0 0 ofs

  let len buf ofs = function
    | `Int     -> int buf ofs
    | `Int8    -> int8 buf ofs
    | `Int16   -> int16 buf ofs
    | `Int32   -> int32 buf ofs >|= Int32.to_int
    | `Int64   -> int64 buf ofs >|= Int64.to_int
    | `Fixed n -> ok ofs n

  let string ~toplevel n buf ofs =
    if toplevel then (
      assert (ofs = 0);
      ok (String.length buf) buf
    ) else (
      len buf ofs n >>= fun (ofs, len) ->
      let str = Bytes.create len in
      String.blit buf ofs str 0 len ;
      ok (ofs+len) (Bytes.unsafe_to_string str)
    )

  let bytes ~toplevel n buf ofs =
    if toplevel then (
      assert (ofs = 0);
      ok (String.length buf) (Bytes.unsafe_of_string buf)
    ) else (
      len buf ofs n >>= fun (ofs, len) ->
      let str = Bytes.create len in
      String.blit buf ofs str 0 len ;
      ok (ofs+len) str
    )

  let list l n buf ofs =
    len buf ofs n >>= fun (ofs, len) ->
    let rec aux acc ofs = function
      | 0 -> ok ofs (List.rev acc)
      | n ->
        l buf ofs >>= fun (ofs, x) ->
        aux (x :: acc) ofs (n - 1)
    in
    aux [] ofs len

  let array l len buf ofs = list l len buf ofs >|= Array.of_list

  let pair a b buf ofs =
    a buf ofs >>= fun (ofs, a) ->
    b buf ofs >|= fun b ->
    (a, b)

  let triple a b c buf ofs =
    a buf ofs >>= fun (ofs, a) ->
    b buf ofs >>= fun (ofs, b) ->
    c buf ofs >|= fun c ->
    (a, b, c)

  let option o buf ofs =
    char buf ofs >>= function
    | ofs, '\000' -> ok ofs None
    | ofs, _      -> o buf ofs >|= fun x -> Some x

  let rec t: type a. a t -> a decode_bin = fun ty ~toplevel -> match ty with
    | Self s    -> t ~toplevel s.self
    | Custom c  -> c.decode_bin ~toplevel
    | Map b     -> map ~toplevel b
    | Prim t    -> prim ~toplevel t
    | List l    -> list (t ~toplevel:false l.v) l.len
    | Array a   -> array (t ~toplevel:false a.v) a.len
    | Tuple t   -> tuple ~toplevel t
    | Option x  -> option (t ~toplevel:false x)
    | Record r  -> record ~toplevel r
    | Variant v -> variant ~toplevel v

  and tuple: type a. a tuple -> a decode_bin = fun ty ~toplevel:_ ->
    let t = t ~toplevel:false in
    match ty with
    | Pair (x,y)     -> pair (t x) (t y)
    | Triple (x,y,z) -> triple (t x) (t y) (t z)

  and map: type a b. (a, b) map -> b decode_bin =
    fun { x; f; _ } ~toplevel buf ofs -> t ~toplevel x buf ofs >|= f

  and prim: type a. a prim -> a decode_bin = fun ty ~toplevel -> match ty with
    | Unit   -> unit
    | Bool   -> bool
    | Char   -> char
    | Int    -> int
    | Int32  -> int32
    | Int64  -> int64
    | Float  -> float
    | String n  -> string ~toplevel n
    | Bytes n   -> bytes ~toplevel n

  and record: type a. a record -> a decode_bin = fun r ~toplevel:_ buf ofs ->
    match r.rfields with
    | Fields (fs, c) ->
      let rec aux: type b. int -> b -> (a, b) fields -> a res
        = fun ofs f -> function
          | F0         -> ok ofs f
          | F1 (h, t) ->
            field ~toplevel:false h buf ofs >>= fun (ofs, x) ->
            aux ofs (f x) t
      in
      aux ofs c fs

  and field: type a  b. (a, b) field -> b decode_bin = fun f -> t f.ftype

  and variant: type a. a variant -> a decode_bin = fun v ~toplevel:_ buf ofs ->
    int buf ofs >>= fun (ofs, i) ->
    case ~toplevel:false v.vcases.(i) buf ofs

  and case: type a. a a_case -> a decode_bin = fun c ~toplevel buf ofs ->
    match c with
    | C0 c -> ok ofs c.c0
    | C1 c -> t ~toplevel c.ctype1 buf ofs >|= c.c1

end

let map_result f = function
  | Ok x -> Ok (f x)
  | Error _ as e -> e

let decode_bin = Decode_bin.t

let of_bin_string t x =
 let rec aux
   : type a. a t -> string -> (a, [`Msg of string]) result
   = fun t x -> match t with
     | Self s          -> aux s.self x
     | Prim (String _) -> Ok x
     | Prim (Bytes _)  -> Ok (Bytes.of_string x)
     | _ ->
       let last, v = Decode_bin.t ~toplevel:true t x 0 in
       assert (last = String.length x);
       Ok v
 in
 try aux t x
 with Invalid_argument e -> Error (`Msg e)

let to_string t = Fmt.to_to_string (pp t)

let of_string t =
  let v f x = try Ok (f x) with Invalid_argument e -> Error (`Msg e) in
  let rec aux: type a a. a t -> a of_string = fun t x ->
      match t with
        | Self s   -> aux s.self x
        | Custom c -> c.of_string x
        | Map l    -> map l x
        | Prim p   -> prim p x
        | _        -> of_json_string t x

  and map: type a b. (a, b) map -> b of_string = fun l x ->
    aux l.x x |> map_result l.f

  and prim: type a. a prim -> a of_string = fun t x ->
    match t with
    | Unit     -> Ok ()
    | Bool     -> v bool_of_string x
    | Char     -> v (fun x -> String.get x 1) x
    | Int      -> v int_of_string x
    | Int32    -> v Int32.of_string x
    | Int64    -> v Int64.of_string x
    | Float    -> v float_of_string x
    | String _ -> Ok x
    | Bytes _  -> Ok (Bytes.unsafe_of_string x)
  in
  aux t

type 'a ty = 'a t

let hash t x = match t with
  | Custom c -> c.hash x
  | _ -> Hashtbl.hash (to_bin_string t x)

module type S = sig
  type t
  val t: t ty
end

let (>|=) = Decode_json.(>|=)

let join = function
  | Error _ as e -> e
  | Ok x         -> x

let like ?cli ?json ?bin ?equal ?compare ?hash:h ?pre_digest:p t =
  let encode_json, decode_json = match json with
    | Some (x, y) -> x, y
    | None ->
      let string = Prim (String `Int) in
      match t, cli with
      | Prim _, Some (pp, of_string) ->
        (fun ppf u -> Encode_json.t string ppf (Fmt.to_to_string pp u)),
        (fun buf -> Decode_json.t string buf >|= of_string |> join)
      | _ ->Encode_json.t t, Decode_json.t t
  in
  let pp, of_string = match cli with
    | Some (x, y) -> x, y
    | None -> pp t, of_string t
  in
  let encode_bin, decode_bin, size_of = match bin with
    | Some (x, y, z) -> x, y, z
    | None -> encode_bin t, decode_bin t, size_of t
  in
  let equal = match equal with
    | Some x -> x
    | None -> match compare with
      | Some f -> (fun x y -> f x y = 0)
      | None   -> Equal.t t
  in
  let compare = match compare with
    | Some x -> x
    | None -> Compare.t t
  in
  let hash = match h with
    | Some x -> x
    | None -> hash t
  in
  let pre_digest = match p with
    | Some x -> (fun y -> Fmt.epr "XXX UUU\n%!"; x y)
    | None -> (fun x -> Fmt.epr "XXX XXX\n%!"; to_bin size_of encode_bin x)
  in
  Custom {
    cwit = `Type t;
    pp; of_string; pre_digest;
    encode_json; decode_json;
    encode_bin; decode_bin; size_of;
    compare; equal; hash
  }

let map ?cli ?json ?bin ?equal ?compare ?hash ?pre_digest x f g =
  match cli, json, bin, equal, compare, hash, pre_digest with
  | None, None, None, None, None, None, None ->
    Map { x; f; g; mwit = Witness.make () }
  | _ ->
    let x = Map { x; f; g; mwit = Witness.make () } in
    like ?cli ?json ?bin ?equal ?compare ?hash ?pre_digest x

let pre_digest t x =
  let rec aux: type a . a t -> a -> string = fun t x ->
    match t with
    | Self s   -> aux s.self x
    | Map m    -> aux m.x (m.g x)
    | Custom c -> c.pre_digest x
    | _ -> to_bin_string t x
  in
  aux t x
