{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TypeApplications #-}
-- | Very partial implementation of the Python Pickle Virtual Machine
-- (protocol 2): i.e. parses pickled data into opcodes, then executes the
-- opcodes to construct a (Haskell representation of a) Python object.
module Language.Python.Pickle where

import Control.Applicative ((<$>), (<*), (*>))
import Control.Monad.State
import Control.Monad.Writer
import qualified Data.ByteString as S
import qualified Data.ByteString.Char8 as SC
import qualified Data.ByteString.Builder as SB
import qualified Data.ByteString.Builder.Prim as SBP
import qualified Data.ByteString.Lazy as LB
import Data.Attoparsec.ByteString hiding (parse, take)
import qualified Data.Attoparsec.ByteString.Char8 as AC
import qualified Data.Attoparsec.ByteString as A
import qualified Data.Attoparsec.Binary as A
import Data.Attoparsec.ByteString.Char8 (decimal, double, signed)
import Data.Int (Int32)
import qualified Data.Char as C
import Numeric.Natural
import Data.Bits
import Data.IntMap (IntMap)
import qualified Data.IntMap as IM
import Data.List (foldl', unfoldr)
import Data.Map (Map)
import qualified Data.Map as M
import Data.Serialize.Get (getWord16le, getWord32le, runGet)
import Data.Serialize.IEEE754
import qualified Data.Text.Lazy.Builder as TB
import qualified Data.Text.Lazy as TL
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Word (Word32, Word64, Word16, Word8)
import GHC.Generics



-- | Parse a pickled object to a list of opcodes.
parse :: S.ByteString -> Either String [OpCode]
parse = parseOnly (many1 (choice opcodes) <* endOfInput)

-- | Unpickle (i.e. deserialize) a Python object. Protocols 0, 1, and 2 are
-- supported.
unpickle :: S.ByteString -> Either String Value
unpickle s = do
  xs <- parse s
  unpickle' xs

-- | Pickle (i.e. serialize) a Python object. Protocol 2 is used.
pickle :: Value -> S.ByteString
pickle value = LB.toStrict . SB.toLazyByteString . mconcat . map serialize . (PROTO 2:) . (++ [STOP]) . runPickler $ pickle' value

----------------------------------------------------------------------
-- Pickle opcodes parser
----------------------------------------------------------------------

-- TODO parsing could be done with a big switch and only cereal,
-- instead of relying on attoparsec's "choice" combinator.

opcodes :: [Parser OpCode]
opcodes =
  -- true and false are in fact special cases for the int parser,
  -- It is important they are tried before int.
  [ true, false, int, binint, binint1, binint2, long, long1, long4
  , string', binstring, short_binstring
  , none
  , newtrue, newfalse
  , unicode, binunicode
  , binbytes, short_binbytes
  , float, binfloat
  , empty_list, append, appends, list
  , empty_tuple, tuple, tuple1, tuple2, tuple3
  , empty_dict, dict, setitem, setitems
  , pop, dup, mark, popmark
  , get', binget, long_binget, put', binput, long_binput
  , ext1, ext2, ext4
  , global, reduce, build, inst, obj, newobj
  , proto, stop
  , persid, binpersid
  ]

-- Integers

int, binint, binint1, binint2, long, long1, long4 :: Parser OpCode
int = string "I" *> (INT <$> decimalInt)
binint = string "J" *> (BININT <$> int4)
binint1 = string "K" *> (BININT1 <$> anyWord8)
binint2 = string "M" *> (BININT2 <$> A.anyWord16le)
long = string "L" *> (LONG <$> decimalLong)
long1 = string "\138" *> (LONG1 <$> decodeLong1) -- same as \x8a
long4 = string "\139" *> (LONG4 <$> decodeLong4) -- same as \x8b

-- Strings

string', binstring, short_binstring :: Parser OpCode
string' = string "S" *> (STRING <$> stringnl)
binstring = string "T" *> (BINSTRING <$> string4)
short_binstring = string "U" *> (SHORT_BINSTRING <$> string1)

-- Bytes
binbytes, short_binbytes :: Parser OpCode
binbytes = string "B" *> (BINBYTES <$> bytes4)
short_binbytes = string "C" *> (SHORT_BINBYTES <$> bytes1)
-- None

none :: Parser OpCode
none = string "N" *> return NONE

-- Booleans

true, false, newtrue, newfalse :: Parser OpCode
true = string "I01\n" *> return NEWTRUE
false = string "I00\n" *> return NEWFALSE
newtrue = string "\136" *> return NEWTRUE -- same as \x88
newfalse = string "\137" *> return NEWFALSE -- same as \x89

-- Unicode strings

unicode, binunicode :: Parser OpCode
unicode = string "V" *> (UNICODE <$> unicodestringnl)
binunicode = string "X" *> (BINUNICODE <$> unicodestring4)

-- Floats

float, binfloat :: Parser OpCode
float = string "F" *> (FLOAT <$> doubleFloat)
binfloat = string "G" *> (BINFLOAT <$> float8)

-- Lists

empty_list, append, appends, list :: Parser OpCode
empty_list = string "]" *> return EMPTY_LIST
append = string "a" *> return APPEND
appends = string "e" *> return APPENDS
list = string "l" *> return LIST

-- Tuples

empty_tuple, tuple, tuple1, tuple2, tuple3 :: Parser OpCode
empty_tuple = string ")" *> return EMPTY_TUPLE
tuple = string "t" *> return TUPLE
tuple1 = string "\133" *> return TUPLE1 -- same as \x85
tuple2 = string "\134" *> return TUPLE2 -- same as \x86
tuple3 = string "\135" *> return TUPLE3 -- same as \x87

-- Dictionaries

empty_dict, dict, setitem, setitems :: Parser OpCode
empty_dict = string "}" *> return EMPTY_DICT
dict = string "d" *> return DICT
setitem = string "s" *> return SETITEM
setitems = string "u" *> return SETITEMS

-- Stack manipulation

pop, dup, mark, popmark :: Parser OpCode
pop = string "0" *> return POP
dup = string "2" *> return DUP
mark = string "(" *> return MARK
popmark = string "1" *> return POP_MARK

-- Memo manipulation

get', binget, long_binget, put', binput, long_binput :: Parser OpCode
get' = string "g" *> (GET <$> unsignedDecimalInt)
binget = string "h" *> (BINGET . fromIntegral <$> anyWord8)
long_binget = string "j" *> (LONG_BINGET . fromIntegral <$> uint4)
put' = string "p" *> (PUT <$> unsignedDecimalInt)
binput = string "q" *> (BINPUT . fromIntegral <$> anyWord8)
long_binput = string "r" *> (LONG_BINPUT . fromIntegral <$> uint4)

-- Extension registry (predefined objects)

ext1, ext2, ext4 :: Parser OpCode
ext1 = string "\130" *> (EXT1 . fromIntegral <$> uint1) -- same as \x82
ext2 = string "\131" *> (EXT2 . fromIntegral <$> uint2) -- same as \x83
ext4 = string "\132" *> (EXT4 . fromIntegral <$> uint4) -- same as \x84

-- Various

global, reduce, build, inst, obj, newobj :: Parser OpCode
global = string "c" *> (GLOBAL <$> stringnl_noescape <*> stringnl_noescape)
reduce = string "R" *> return REDUCE
build = string "b" *> return BUILD
inst = string "i" *> (INST <$> stringnl_noescape <*> stringnl_noescape)
obj = string "o" *> return OBJ
newobj = string "\129" *> return NEWOBJ -- same as \x81

-- Machine control

proto, stop :: Parser OpCode
proto = string "\128" *> (PROTO . fromIntegral <$> anyWord8)
stop = string "." *> return STOP

-- Persistent IDs

persid, binpersid :: Parser OpCode
persid = string "P" *> (PERSID <$> stringnl_noescape)
binpersid = string "Q" *> return BINPERSID

-- Basic parsers

decimalInt :: Parser Int
decimalInt = signed decimal <* string "\n"

unsignedDecimalInt :: Parser Natural
unsignedDecimalInt = decimal <* string "\n"

-- TODO document the differences with Python's representation.
doubleFloat :: Parser Double
doubleFloat = choice [double, string "inf" *> pure (1/0), string "-inf" *> pure (-1/0), string "nan" *> pure (0/0)] <* string "\n"

decimalLong :: Parser Integer
decimalLong = signed decimal <* string "L\n"

decodeLong1 :: Parser Integer
decodeLong1 = do
  n <- fromIntegral <$> anyWord8
  if n > 0
    then do
      ns <- A.take n
      let a = toLong ns
      return $ if S.last ns > 127 then negate $ 256 ^ S.length ns - a else a
    else return 0
  where toLong = S.foldr' (\w a -> (a * 256 + toInteger w)) 0

integerToInts :: Integer -> [Int]
integerToInts = unfoldr folder
      where
       intMax = toInteger (maxBound :: Int)
       folder x | x > intMax = Just (maxBound, x - intMax)
                | x > 0 = Just (fromInteger x, 0)
                | otherwise = Nothing
decodeLong4 :: Parser Integer
decodeLong4 = do
  n' <- runGet getWord32le <$> A.take 4
  n <- either fail return n'
  if n > 0
    then do
      ns <- mapM A.take . integerToInts $ toInteger n
      let a = toLong ns
      return $ if (S.last .last) ns > 127 then negate $ 256 ^ (sum . map S.length) ns - a else a
    else return 0
  where toLong = foldr (\n a -> a * toInteger (maxBound :: Int) + S.foldr' (\w a' -> a' * 256 + toInteger w) 0 n) 0

unicodestring4 :: Parser Text
unicodestring4 = do
  n' <- runGet getWord32le <$> A.take 4
  n <- either fail return n'
  if n > 0 then decodeUtf8 <$> A.take (fromIntegral n) else return mempty
string1 :: Parser S.ByteString
string1 = do
  n <- fromIntegral <$> anyWord8
  A.take n
string4 :: Parser S.ByteString
string4 = do
  n' <- runGet getWord32le <$> A.take 4
  n <- either fail return n'
  if n > 0 then A.take (fromIntegral n) else return mempty
stringnl :: Parser S.ByteString
stringnl = do
  open <- quote
  LB.toStrict . SB.toLazyByteString . mconcat <$> manyTill (charOrEscape open) (AC.char open)  <* AC.char '\n'
      where 
        charOrEscape q = choice [unescapedChunk, escapeSeq]
          where
            unescapedChunk = SB.byteString <$> AC.takeWhile1 (\c -> c /= '\\' && c /= q)
            escapeSeq = do
              c <- AC.char '\\' *> AC.peekChar'
              case c of
                '\\' -> "\\" <$ anyWord8
                '\'' -> "'" <$ anyWord8
                '"'  -> "\"" <$ anyWord8
                'a'  -> "\a" <$ anyWord8
                'b'  -> "\b" <$ anyWord8
                'f'  -> "\f" <$ anyWord8
                'n'  -> "\n" <$ anyWord8
                'r'  -> "\r" <$ anyWord8
                't'  -> "\t" <$ anyWord8
                'v'  -> "\v" <$ anyWord8
                'x'  -> anyWord8 *> (SB.word8 <$> hexadecimalExact 2)
                x | isOctDigit_w8 (fromEnum x) -> SB.word8 <$> octalLim 3
                  | otherwise -> fail $ "Unsupported escape: \\" ++ show x
        quote = AC.satisfy $ \x -> x == '\'' || x == '"'
unicodestringnl :: Parser Text
unicodestringnl = TL.toStrict . TB.toLazyText . mconcat <$> manyTill charOrEscape (AC.char '\n')
      where 
        charOrEscape = choice [unescapedChunk, escapeSeq]
          where
            unescapedChunk = do
              chunk <- decodeUtf8' <$> AC.takeWhile1 (\c -> c /= '\\' && c /= '\n')
              either (fail . show) (return . TB.fromText) chunk
            escapeSeq = do
              c <- AC.char '\\' *> AC.anyChar
              case c of
                'u' -> unquote $ hexadecimalExact 4
                'U' -> unquote $ hexadecimalExact 8
                x -> return ("\\" <> TB.singleton x)
        unquote p = TB.singleton . toEnum <$> p
octalLim :: (Bits a, Integral a) => Int -> Parser a
octalLim maxcnt = fst . snd <$> A.runScanner (0,maxcnt) step
  where step (total, cnt) w = if isOctDigit_w8 w && cnt > 0 then Just ((total `shiftL` 3) .|. fromIntegral (w - 48), cnt - 1) else Nothing
hexadecimalLim :: (Bits a, Integral a) => Int -> Parser a
hexadecimalLim maxcnt = fst . snd <$> A.runScanner (0,maxcnt) step
  where step (total, cnt) w = if isHexDigit w && cnt > 0 then Just (step' total w, cnt - 1) else Nothing
        isHexDigit w = (w >= 48 && w <= 57) ||
                      (w >= 97 && w <= 102) ||
                      (w >= 65 && w <= 70)
        step' a w | w >= 48 && w <= 57  = (a `shiftL` 4) .|. fromIntegral (w - 48)
                | w >= 97             = (a `shiftL` 4) .|. fromIntegral (w - 87)
                | otherwise           = (a `shiftL` 4) .|. fromIntegral (w - 55)

hexadecimalExact :: (Bits a, Integral a) => Int -> Parser a
hexadecimalExact maxcnt = do 
  (res, finalCount) <- snd <$> A.runScanner (0,maxcnt) step
  if finalCount > 0 then fail "Malformed decimal" else return res
  where step (total, cnt) w = if isHexDigit w && cnt > 0 then Just (step' total w, cnt - 1) else Nothing
        isHexDigit w = (w >= 48 && w <= 57) ||
                        (w >= 97 && w <= 102) ||
                        (w >= 65 && w <= 70)
        step' a w | w >= 48 && w <= 57  = (a `shiftL` 4) .|. fromIntegral (w - 48)
                | w >= 97             = (a `shiftL` 4) .|. fromIntegral (w - 87)
                | otherwise           = (a `shiftL` 4) .|. fromIntegral (w - 55)

isOctDigit_w8 :: (Ord a, Num a) => a -> Bool
isOctDigit_w8 w = w - 48 <= 7

stringnl_noescape :: Parser S.ByteString
stringnl_noescape = takeTill (== toEnum 10) <* word8 10
float8 :: Parser Double
float8 = do
  w <- runGet getFloat64be <$> A.take 8
  either fail return w

bytes1 :: Parser S.ByteString
bytes1 = do
  w <- anyWord8
  A.take (fromIntegral w)
bytes4 :: Parser S.ByteString
bytes4 = do
  w <- runGet getWord32le <$> A.take 4
  either fail (A.take . fromIntegral) w
int4 :: Parser Int32
int4 = fromIntegral <$> A.anyWord32le

uint1 :: Parser Word8
uint1 = anyWord8
uint2 :: Parser Word16
uint2 = do
  w <- runGet getWord16le <$> A.take 2
  case w of
    Left err -> fail err
    Right x -> return $ fromIntegral x
uint4 :: Parser Word32
uint4 = A.anyWord32le
----------------------------------------------------------------------
-- Pickle opcodes serialization
----------------------------------------------------------------------

serialize :: OpCode -> SB.Builder
serialize opcode = case opcode of
  PROTO i -> SB.word8 0x80 <> SB.word8 i
  STOP -> "."
  DUP -> "2"
  POP -> "0"
  POP_MARK -> "1"
  GLOBAL mod attr -> "c" <> SB.byteString mod <> "\n" <> SB.byteString attr <> "\n"
  BUILD -> "b"
  INST mod attr -> "i" <> SB.byteString mod <> "\n" <> SB.byteString attr <> "\n"
  OBJ -> "o"
  REDUCE -> "R"
  PERSID x -> "P" <> SB.byteString x <> "\n"
  BINPERSID -> "Q"
  NEWOBJ -> SB.word8 0x81
  NEWTRUE -> SB.word8 0x88
  NEWFALSE -> SB.word8 0x89
  GET i -> "g" <> SB.integerDec (fromIntegral i) <> "\n"
  BINGET i -> "h" <> SB.word8 (fromIntegral i)
  LONG_BINGET i -> "j" <> SB.word32LE (fromIntegral i)
  PUT i -> "p" <> SB.integerDec (fromIntegral i) <> "\n"
  BINPUT i -> "q" <> SB.word8 i
  LONG_BINPUT i -> "r" <> SB.word32LE i
  BININT i -> "J" <> SB.int32LE (fromIntegral i)
  BININT1 i -> "K" <> SB.word8 i
  BININT2 i -> "M" <> SB.word16LE i
  NONE -> "N"
  INT i -> "I" <> SB.intDec i <> "\n"
  FLOAT f -> "F" <> encodeFloatnl f
  LONG i -> "L" <> SB.integerDec i <> "L\n"
  LONG1 0 -> SB.word8 0x8a <> SB.word8 0x00
  LONG1 i -> SB.word8 0x8a <> encodeLong1 i
  LONG4 i -> SB.word8 0x8b <> encodeLong4 i
  BINFLOAT d -> "G" <> SB.doubleBE d
  STRING s -> "S'" <> encodeString s <> "'\n"
  SHORT_BINSTRING s -> "U" <> encBSWithLen SB.word8 s
  BINSTRING s -> "T" <> encBSWithLen SB.word32LE s
  UNICODE s -> "V" <> encodeUnicode s <> "\n"
  BINUNICODE s -> "X" <> encBSWithLen SB.word32LE (encodeUtf8 s)
  SHORT_BINBYTES b -> "C" <> encBSWithLen SB.word8 b
  BINBYTES b -> "B" <> encBSWithLen SB.word32LE b
  EMPTY_DICT -> "}"
  EMPTY_LIST -> "]"
  EMPTY_TUPLE -> ")"
  LIST -> "l"
  TUPLE -> "t"
  TUPLE1 -> SB.word8 0x85
  TUPLE2 -> SB.word8 0x86
  TUPLE3 -> SB.word8 0x87
  MARK -> "("
  DICT -> "d"
  SETITEM -> "s"
  SETITEMS -> "u"
  APPEND -> "a"
  APPENDS -> "e"
  EXT1 x -> SB.word8 0x82 <> SB.word8 x
  EXT2 x -> SB.word8 0x83 <> SB.word16LE x
  EXT4 x -> SB.word8 0x84 <> SB.word32LE x
  x -> error $ "serialize: " ++ show x

encBSWithLen :: Integral a => (a -> SB.Builder) -> S.ByteString -> SB.Builder
encBSWithLen encodeLen s = encodeLen (fromIntegral . S.length $ s) <> SB.byteString s

encodeUnicode :: Text -> SB.Builder
encodeUnicode = SBP.primUnfoldrBounded escape T.uncons
  where
    uEscape = SBP.condB (\c -> fromEnum c < fromIntegral (maxBound :: Word16)) (escapeWith 'u' SBP.int16HexFixed) (escapeWith 'U' SBP.int32HexFixed)
    escapeWith c enc = SBP.liftFixedToBounded $ (\x -> ('\\', (c, fromIntegral $ fromEnum x))) SBP.>$< SBP.char7 SBP.>*< SBP.char7 SBP.>*< enc
    escape = SBP.condB (\c -> fromEnum c > 127 || c == '\n' || c == '\\') uEscape (SBP.liftFixedToBounded SBP.char7) 

encodeString :: S.ByteString -> SB.Builder
encodeString = SC.foldl escape mempty
    where 
      escape b c = b <> escape' c
      escape' c = case c of
        '\\' -> "\\\\"
        '\'' -> "\\'"
        '\a' -> "\\a"
        '\b' -> "\\b"
        '\f' -> "\\f"
        '\n' -> "\\n"
        '\r' -> "\\r"
        '\t' -> "\\t"
        '\v' -> "\\v"
        x | C.ord x < 20 || C.ord x > 127 -> "\\x" <> SB.word8HexFixed (fromIntegral $ C.ord x)
          | otherwise -> SB.char7 c

encodeFloatnl :: Double -> SB.Builder
encodeFloatnl d | isInfinite d && d > 0 = "inf\n"
              | isInfinite d && d < 0 = "-inf\n"
              | isNaN d = "nan\n"
              | otherwise = SB.doubleDec d <> "\n"

encodeLong1 :: Integer -> SB.Builder
encodeLong1 = encBSWithLen SB.word8 . i2bs

encodeLong4 :: Integer -> SB.Builder
encodeLong4 = encBSWithLen SB.word32LE . i2bs
bs2i :: S.ByteString -> Integer
bs2i b
   | sign = go b - 2 ^ (S.length b * 8)
   | otherwise = go b
   where
      go = S.foldl' (\i b -> (i `shiftL` 8) + fromIntegral b) 0
      sign = S.index b 0 > 127

integerBytes :: Integer -> Int
integerBytes x = (integerLogBase 2 (abs x) + 1) `quot` 8 + 1

i2bs :: Integer -> S.ByteString
i2bs x
   | x == 0 = S.singleton 0
   | x == (-1) = S.singleton 255
   | x < 0 = i2bs' (2 ^ (8 * bytes) + x) <> if needFreshByte then "\xff" else mempty
   | otherwise = i2bs' x <> if needFreshByte then "\0" else mempty
   where
      needFreshByte = topBit `mod` 8 == 0
      topBit = integerLogBase 2 (if x < 0 then abs x - 1 else x)  + 1
      i2bs' = S.unfoldr go
      bytes = integerBytes x
      go 0 = Nothing
      go 255 = Nothing
      go i = Just (fromIntegral i, i `shiftR` 8)

integerLogBase :: Integer -> Integer -> Int
integerLogBase b i =
     if i < b then
        0
     else
        -- Try squaring the base first to cut down the number of divisions.
        let l = 2 * integerLogBase (b*b) i
            doDiv :: Integer -> Int -> Int
            doDiv i l = if i < b then l else doDiv (i `div` b) (l+1)
        in  doDiv (i `div` (b^l)) l
        
----------------------------------------------------------------------
-- Pickle opcodes
----------------------------------------------------------------------

data OpCode =
  -- Integers
    INT Int
  | BININT Int32
  | BININT1 Word8
  | BININT2 Word16
  | LONG Integer
  | LONG1 Integer
  | LONG4 Integer

  -- Strings
  | STRING S.ByteString
  | BINSTRING S.ByteString
  | SHORT_BINSTRING S.ByteString

  -- None
  | NONE

  -- Booleans
  | NEWTRUE
  | NEWFALSE

  -- Unicode strings
  | UNICODE Text -- TODO (use Text ?)
  | BINUNICODE Text
  -- Bytes
  | BINBYTES S.ByteString
  | SHORT_BINBYTES S.ByteString
  -- Floats
  | FLOAT Double
  | BINFLOAT Double

  -- Lists
  | EMPTY_LIST
  | APPEND
  | APPENDS
  | LIST

  -- Tuples
  | EMPTY_TUPLE
  | TUPLE
  | TUPLE1
  | TUPLE2
  | TUPLE3

  -- Dictionaries
  | EMPTY_DICT
  | DICT
  | SETITEM
  | SETITEMS

  -- Stack manipulation
  | POP
  | DUP
  | MARK
  | POP_MARK

  -- Memo manipulation
  | GET Natural
  | BINGET Word8
  | LONG_BINGET Word32
  | PUT Natural
  | BINPUT Word8
  | LONG_BINPUT Word32

  -- Extension registry (predefined objects)
  | EXT1 Word8
  | EXT2 Word16
  | EXT4 Word32

  -- Various
  | GLOBAL S.ByteString S.ByteString
  | REDUCE
  | BUILD
  | INST S.ByteString S.ByteString
  | OBJ
  | NEWOBJ

  -- Pickle machine control
  | PROTO Word8 -- in [2..255]
  | STOP

  -- Persistent IDs
  | PERSID S.ByteString
  | BINPERSID
  deriving (Show, Generic, Eq)

{-
protocol0 = [INT, LONG, STRING, NONE, UNICODE, FLOAT, APPEND, LIST, TUPLE,
  DICT, SETITEM, POP, DUP, MARK, GET, PUT, GLOBAL, REDUCE, BUILD, INST, STOP,
  PERSID]
protocol1 = [BININT, BININT1, BININT2, BINSTRING, SHORT_BINSTRING, BINUNICODE,
  BINFLOAT, EMPTY_LIST, APPENDS, EMPTY_TUPLE, EMPTY_DICT, SETITEMS, POP_MARK,
  BINGET, LONG_BINGET, BINPUT, LONG_BINPUT, OBJ, BINPERSID]
protocol2 = [LONG1, LONG4, NEWTRUE, NEWFALSE, TUPLE1, TUPLE2, TUPLE3, EXT1,
  EXT2, EXT4, NEWOBJ, PROTO]
-}

----------------------------------------------------------------------
-- Pyhon value representation
----------------------------------------------------------------------

-- Maybe I can call them Py? And Have IsString/Num instances?
data Value =
    Dict (Map Value Value)
  | List [Value]
  | Tuple [Value]
  | None
  | Bool Bool
  | BinInt Int
  | BinLong Integer
  | BinFloat Double
  | BinString S.ByteString
  | BinUnicode Text
  | BinBytes S.ByteString
  | Memorable Value
  deriving (Eq, Ord, Show)

----------------------------------------------------------------------
-- Pickle machine (opcodes to value)
----------------------------------------------------------------------

unpickle' :: [OpCode] -> Either String Value
unpickle' xs = execute xs [] (IM.empty)

type Stack = [[Value]]

type Memo = IntMap Value

data UnpicklerState = US {usStack :: Stack, usMemo :: Memo}
newtype Unpickler a = UnPickler { runUnP :: StateT UnpicklerState (Either String) a}
  deriving (Functor, Applicative, Monad, MonadState UnpicklerState)

pushS :: Value -> Unpickler ()
pushS v = modify $ \s -> s{usStack = push' (usStack s)}
  where
    push' (s:ss) = (v:s):ss
    push' [] = [[v]]
popS :: Unpickler Value
popS = do 
  s <- get
  case usStack s of
    ((v:topStack):ss) -> do
      put s{usStack = topStack:ss}
      return v
    _ -> fail "popS: Tried to pop an empty stack"
peekS :: Unpickler Value
peekS = do
  s <- gets usStack
  case s of
    ((v:_):_) -> return v
    _ -> fail "peekS: Tried to pop an empty stack"
popToMark :: Unpickler [Value]
popToMark = do
  s <- get
  case usStack s of 
    (s':ss) -> do
      put s{usStack = ss}
      return $ reverse s'
    _ -> fail "popToMark: Tried to pop an empty stack"

popN :: Int -> Unpickler [Value]
popN n = do
  s <- get
  case usStack s of
    (s':ss) | length s' >= n -> do
      let (vs, s'') = splitAt n s'
      put s{usStack = s'':ss}
      return $ reverse vs
    _ -> fail "popN: Tried to pop an insufficient stack"
pushMark :: Unpickler ()
pushMark = modify $ \s -> s{usStack = []:usStack s}

remember :: Integral a => a -> Value -> Unpickler ()
remember i v = modify $ \s -> s{usMemo = IM.insert (fromIntegral i) v $ usMemo s}

toPairs :: Monad m => [a] -> m [(a, a)]
toPairs (a:b:xs) = ((a,b):) <$> toPairs xs
toPairs [_] = fail "toPairs: Can't pair up odd list"
toPairs []       = return []

execute :: [OpCode] -> Stack -> Memo -> Either String Value
execute [] [[value]] _ = Right value
execute (op:ops) stack memo = case flip runStateT (US stack memo) . runUnP $ executeOne op of
  Left err              -> Left err
  Right ((), US stack' memo') -> execute ops stack' memo'
execute ops stack _ = Left $ "`execute` unimplemented: (stack:  " ++ show stack ++ ", ops: " ++ show ops ++ ")"

executePartial :: [OpCode] -> Stack -> Memo -> (Stack, Memo, [OpCode])
executePartial [] stack memo = (stack, memo, [])
executePartial (op:ops) stack memo = case flip runStateT (US stack memo) . runUnP $ executeOne op of
  Left _ -> (stack, memo, op:ops)
  Right ((), US stack' memo') -> executePartial ops stack' memo'


executeLog :: [OpCode] -> Stack -> Memo -> (Stack, Memo, [OpCode], [(OpCode, Stack)])
executeLog [] stack memo = (stack, memo, [], [])
executeLog (op:ops) stack memo = case flip runStateT (US stack memo) . runUnP $ executeOne op of
  Left _ -> (stack, memo, op:ops, [])
  Right ((), US stack' memo') -> let (s,m,os,ss) = executeLog ops stack' memo' in
    (s,m,os, (op, stack):ss)

executeOne :: OpCode -> Unpickler ()
executeOne EMPTY_DICT = pushS (Dict M.empty)
executeOne EMPTY_LIST = pushS (List [])
executeOne EMPTY_TUPLE = pushS (Tuple [])
executeOne (PUT i) = peekS >>= remember i
executeOne (GET i) = executeLookup i
executeOne (BINPUT i) = peekS >>= remember i
executeOne (BINGET i) = executeLookup i
executeOne NONE = pushS None
executeOne NEWTRUE = pushS (Bool True)
executeOne NEWFALSE = pushS (Bool False)
executeOne (INT i) = pushS (BinInt i)
executeOne (BININT i) = pushS (BinInt $ fromIntegral i)
executeOne (BININT1 i) = pushS (BinInt $ fromIntegral i)
executeOne (BININT2 i) = pushS (BinInt $ fromIntegral i)
executeOne (LONG i) = pushS (BinLong i)
executeOne (LONG1 i) = pushS (BinLong i)
executeOne (FLOAT d) = pushS (BinFloat d)
executeOne (BINFLOAT d) = pushS (BinFloat d)
executeOne (STRING s) = pushS (BinString s)
executeOne (SHORT_BINSTRING s) = pushS (BinString s)
executeOne (BINUNICODE s) = pushS (BinUnicode s)
executeOne (UNICODE s) = pushS (BinUnicode s)
executeOne (BINBYTES b) = pushS (BinBytes b)
executeOne (SHORT_BINBYTES b) = pushS (BinBytes b)
executeOne MARK = pushMark
executeOne TUPLE = executeTuple
executeOne TUPLE1 = executeTupleN 1
executeOne TUPLE2 = executeTupleN 2
executeOne TUPLE3 = executeTupleN 3
executeOne DICT = executeDict
executeOne SETITEM = executeSetitem
executeOne SETITEMS = executeSetitems
executeOne LIST = executeList
executeOne APPEND = executeAppend
executeOne APPENDS = executeAppends
executeOne (PROTO _) = return ()
executeOne STOP = return ()
executeOne op = fail $ "Can't execute opcode " ++ show op ++ "."

executeLookup :: Integral a => a -> Unpickler ()
executeLookup k = do 
  memo <- gets usMemo
  case IM.lookup (fromIntegral k) memo of
    Nothing -> fail "Unknown memo key"
    Just s -> pushS s

executeTuple :: Unpickler ()
executeTuple = do
  vals <- popToMark
  pushS (Tuple vals)

executeTupleN :: Int -> Unpickler ()
executeTupleN n = do
  vals <- popN n
  pushS (Tuple vals)

executeDict :: Unpickler ()
executeDict  = do
  vals <- popToMark >>= toPairs
  pushS (vals `addToDict` M.empty)

executeList :: Unpickler ()
executeList = do
  vals <- popToMark
  pushS (List vals)

executeSetitem :: Unpickler ()
executeSetitem = do
  v <- popS
  k <- popS
  o <- popS
  case o of
    Dict d -> pushS (Dict (M.insert k v d))
    _ -> fail "executeSetItem: Can't push into a non-dictionary"

executeSetitems :: Unpickler ()
executeSetitems = do
  vals <- popToMark
  vals' <- toPairs vals
  o <- popS
  case o of
    Dict d -> pushS $ vals' `addToDict` d
    _ -> fail "executeSetitems: Can't push into a non-dictionary"

executeAppend :: Unpickler ()
executeAppend = do
  x <- popS
  o <- popS
  case o of
    List ls -> pushS (List $ ls ++ [x])
    _ -> fail "executeAppend: Can't append into a non-list"

executeAppends :: Unpickler ()
executeAppends = do
  xs <- popToMark
  o <- popS
  case o of
    List ls -> pushS (List $ ls ++ xs)
    _ -> fail "executeAppends: Can't append into a non-list"

addToDict :: [(Value, Value)] -> Map Value Value -> Value
addToDict l d = Dict $ foldl' add d l
  where add d' (k, v) = M.insert k v d'

----------------------------------------------------------------------
-- Pickling (value to opcodes)
----------------------------------------------------------------------

data PicklerState = PS { memoMap :: Map Value Int, memoizeNext :: Bool, memoizeAll :: Bool}
newtype Pickler a = Pickler { runP :: WriterT [OpCode] (State PicklerState) a }
  deriving (Functor, Applicative, Monad, MonadWriter [OpCode], MonadState PicklerState)

runPickler :: Pickler () -> [OpCode]
runPickler p = evalState (execWriterT (runP p)) (PS M.empty True True)

stepPickler :: Pickler () -> Map Value Int -> ([OpCode], Map Value Int)
stepPickler p m = let (o, m') = runState (execWriterT (runP p)) (PS m True True) in (o, memoMap m')

pickle' :: Value -> Pickler ()
pickle' value = do
  m <- gets memoMap
  case M.lookup value m of
    Just k -> tell [BINGET (fromIntegral k)]
    Nothing -> case value of
      Dict d -> pickleDict d
      Memorable v  -> modify (\s -> s{memoizeNext = True}) >> pickle' v
      List xs -> pickleList xs
      Tuple xs -> pickleTuple xs
      None -> tell [NONE]
      Bool True -> tell [NEWTRUE]
      Bool False -> tell [NEWFALSE]
      BinInt i -> pickleBinInt (fromIntegral i)
      BinLong i -> pickleBinLong i
      BinFloat d -> pickleBinFloat d
      BinString s -> pickleBinString s
      BinUnicode s -> pickleBinUnicode s
      x            -> error $ "TODO: pickle " ++ show x

binput' :: Value -> Pickler ()
binput' value = do
  shouldStore <- gets memoizeNext
  when shouldStore $ do
    i <- gets $ M.size . memoMap
    modify $ \s -> s{memoMap = M.insert value i (memoMap s), memoizeNext = memoizeAll s}
    case i of
      x | x < fromIntegral (maxBound :: Word8) -> tell [BINPUT (fromIntegral x)]
        | x < fromIntegral (maxBound :: Word32) -> tell [LONG_BINPUT (fromIntegral x)]
        | otherwise -> tell [PUT (fromIntegral x)]

pickleDict :: Map Value Value -> Pickler ()
pickleDict d = do
  tell [EMPTY_DICT]
  binput' (Dict d)

  let kvs = M.toList d
  case kvs of
    [] -> return ()
    [(k,v)] -> pickle' k >> pickle' v >> tell [SETITEM]
    _ -> do
      tell [MARK]
      mapM_ (\(k, v) -> pickle' k >> pickle' v) kvs
      tell [SETITEMS]

pickleList :: [Value] -> Pickler ()
pickleList xs = do
  shouldStore <- gets memoizeNext
  if shouldStore || null xs then do
  tell [EMPTY_LIST]
  binput' (List xs)
  case xs of
    [] -> return ()
    [x] -> pickle' x >> tell [APPEND]
    _ -> do
      tell [MARK]
      mapM_ pickle' xs
      tell [APPENDS]
   else
    case xs of
        [x] -> tell [EMPTY_LIST] >> pickle' x >> tell [APPEND]
        _ -> do
            tell [MARK]
            mapM_ pickle' xs
            tell [LIST]

pickleTuple :: [Value] -> Pickler ()
pickleTuple [] = tell [EMPTY_TUPLE]
pickleTuple [a] = do
  pickle' a
  tell [TUPLE1]
  binput' (Tuple [a])
pickleTuple [a, b] = do
  pickle' a
  pickle' b
  tell [TUPLE2]
  binput' (Tuple [a, b])
pickleTuple [a, b, c] = do
  pickle' a
  pickle' b
  pickle' c
  tell [TUPLE3]
  binput' (Tuple [a, b, c])
pickleTuple xs = do
  tell [MARK]
  mapM_ pickle' xs
  tell [TUPLE]
  binput' (Tuple xs)

pickleBinInt :: Int32 -> Pickler ()
pickleBinInt i | i >= 0 && i < 256 = tell [BININT1 (fromIntegral i)]
               | i >= 256 && i < 65536 = tell [BININT2 (fromIntegral i)]
               | otherwise = tell [BININT i]

pickleBinLong :: Integer -> Pickler ()
pickleBinLong i = tell [LONG1 i] -- TODO LONG/LONG1/LONG4

-- TODO probably depends on the float range
pickleBinFloat :: Double -> Pickler ()
pickleBinFloat d = tell [BINFLOAT d]

pickleBinString :: S.ByteString -> Pickler ()
pickleBinString s = do
  tell $ if S.length s < 256 then
            [SHORT_BINSTRING s]
            else
            [BINSTRING s]
  binput' (BinString s)

pickleBinUnicode :: Text -> Pickler ()
pickleBinUnicode s = do
  tell [BINUNICODE s]
  binput' (BinUnicode s)

----------------------------------------------------------------------
-- Manipulate Values
----------------------------------------------------------------------

dictGet :: Value -> Value -> Either String (Maybe Value)
dictGet (Dict d) v = return $ M.lookup v d
dictGet _ _ = Left "dictGet: not a dict."

dictGet' :: Value -> Value -> Either String Value
dictGet' (Dict d) v = case M.lookup v d of
  Just value -> return value
  Nothing -> Left "dictGet': no such key."
dictGet' _ _ = Left "dictGet': not a dict."

dictGetString :: Value -> S.ByteString -> Either String S.ByteString
dictGetString (Dict d) s = case M.lookup (BinString s) d of
  Just (BinString s') -> return s'
  _ -> Left "dictGetString: no such key."
dictGetString _ _ = Left "dictGetString: not a dict."

