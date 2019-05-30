use regex::Regex;
use serde::de::{self, DeserializeSeed, MapAccess, Visitor};
use serde::{forward_to_deserialize_any, Deserialize};
use std::fmt::{Display, Formatter};

pub fn is_token_char(c: char) -> bool {
    !"\"(),/:;<=>?@[\\]{}".contains(c)
}

#[derive(Debug, PartialEq)]
pub enum Error {
    Message(String),
    Unexpected(char, String),
    EOF,
    TrailingCharacters,
    UnclosedString,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str(&self.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct DirectivesAccess<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    first: bool,
}

impl<'a, 'de> DirectivesAccess<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        DirectivesAccess { de, first: true }
    }

    fn seek_to_key(&mut self) -> Result<bool> {
        let mut comma = false;
        loop {
            self.de.whitespace();

            match self.de.peek_char() {
                Some(',') => {
                    if comma {
                        return Err(Error::Unexpected(',', "ident".to_string()));
                    } else {
                        comma = true;
                        self.de.next_char().unwrap();
                        continue;
                    }
                }
                Some(c) => {
                    if !self.first && !comma {
                        return Err(Error::Unexpected(',', "name".to_string()));
                    } else if !is_token_char(c) {
                        return Err(Error::Unexpected(c, "name".to_string()));
                    } else {
                        self.first = false;
                        return Ok(true);
                    }
                }

                None => return Ok(false),
            }
        }
    }
}

impl<'de, 'a> MapAccess<'de> for DirectivesAccess<'a, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if self.seek_to_key()? {
            seed.deserialize(&mut *self.de).map(Some)
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

pub struct Deserializer<'de> {
    input: &'de str,
}

impl<'de> Deserializer<'de> {
    fn parse_token(&mut self) -> Result<&'de str> {
        let mut key_end = self.input.len();

        for (i, c) in self.input.char_indices() {
            if c == '=' || c == ',' || c == ' ' {
                key_end = i;
                break;
            } else if !is_token_char(c) {
                return Err(Error::Unexpected(c, "ident or `=,`".into()));
            }
        }

        let ret = &self.input[..key_end];
        self.input = &self.input[key_end..];

        Ok(ret)
    }

    fn peek_char(&mut self) -> Option<char> {
        self.input.chars().next()
    }

    fn peek_res(&mut self) -> Result<char> {
        self.peek_char().ok_or(Error::EOF)
    }

    fn peek_expect(&mut self, e: &str) -> Result<char> {
        self.peek_res().and_then(|c| {
            if e.contains(c) {
                Ok(c)
            } else {
                Err(Error::Unexpected(c, e.to_string()))
            }
        })
    }

    pub fn whitespace(&mut self) {
        let mut chars = self.input.chars();
        while let Some(c) = chars.next() {
            if c != ' ' && c != '\t' {
                return;
            }
            self.input = chars.as_str();
        }
    }

    pub fn next_res(&mut self) -> Result<char> {
        self.next_char().ok_or(Error::EOF)
    }

    pub fn next_expect(&mut self, e: &str) -> Result<char> {
        self.next_res().and_then(|c| {
            if e.contains(c) {
                Ok(c)
            } else {
                Err(Error::Unexpected(c, e.to_string()))
            }
        })
    }

    pub fn next_char(&mut self) -> Option<char> {
        let mut chars = self.input.chars();
        let val = chars.next();
        self.input = chars.as_str();
        val
    }

    pub fn parse_string(&mut self) -> Result<String> {
        self.next_expect("=")?;

        self.next_expect("\"")?;

        let end_re = Regex::new(r#"(?:^|[^\\])[^\\](")"#).unwrap();

        let end_match = end_re
            .captures(self.input)
            .ok_or(Error::UnclosedString)?
            .get(1)
            .ok_or(Error::UnclosedString)?;

        let quote_re = Regex::new(r#"\\(.)"#).unwrap();

        let content = &self.input[1..end_match.start()];

        let ret = quote_re.replace(content, "$1").to_string();

        self.input = &self.input[end_match.end()..];

        Ok(ret)
    }

    pub fn parse_unsigned<T>(&mut self) -> Result<T>
    where
        T: std::ops::AddAssign + std::ops::MulAssign + From<u8>,
    {
        self.next_expect("=")?;

        let mut n: T = 0.into();

        let mut chars = self.input.chars();
        while let Some(c) = chars.next() {
            if c == ',' || c == ' ' {
                break;
            }
            n *= 10.into();
            n += (c
                .to_digit(10)
                .ok_or_else(|| Error::Unexpected(c, "0..9".to_string()))? as u8)
                .into();
            self.input = chars.as_str();
        }

        Ok(n)
    }

    pub fn with_str(input: &'de str) -> Self {
        Deserializer { input }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if let Some('=') = self.peek_char() {
            match self.input.chars().nth(1).ok_or(Error::EOF)? {
                '"' => visitor.visit_string(self.parse_string()?),
                c if c.is_ascii_digit() => visitor.visit_u64(self.parse_unsigned()?),
                c => Err(Error::Unexpected(c, "\" | 0..9".to_string())),
            }
        } else {
            self.whitespace();
            match self.peek_expect(",") {
                Ok(_) | Err(Error::EOF) => visitor.visit_bool(true),
                Err(e) => Err(e),
            }
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.whitespace();
        match self.peek_char() {
            Some('=') | Some(',') => visitor.visit_some(self),
            Some(c) => Err(Error::Unexpected(c, "=,".to_string())),
            None => Err(Error::EOF),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.parse_token()?)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(DirectivesAccess::new(self))
    }

    forward_to_deserialize_any! {
        i8 i16 i32 i64 i128 f32 f64 char
        bytes byte_buf bool unit_struct newtype_struct tuple map
            tuple_struct ignored_any str string u8 u16 u32 u64 u128
            seq enum unit
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct CacheControl {
    pub extension: Option<String>,
    pub max_age: Option<u64>,
    pub must_revalidate: bool,
    pub no_cache: bool,
    pub no_store: bool,
    pub no_transform: bool,
    pub private: bool,
    pub proxy_revalidate: bool,
    pub public: bool,
    pub s_maxage: Option<u64>,
}

pub fn with_str<'a, T>(s: &'a str) -> Result<T>
where
    T: Deserialize<'a>,
{
    let mut deserializer = Deserializer::with_str(s);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.input.is_empty() {
        Ok(t)
    } else {
        Err(Error::TrailingCharacters)
    }
}
