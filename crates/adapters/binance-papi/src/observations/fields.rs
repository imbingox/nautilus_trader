// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Lossless field classification before economic validation or domain projection.

use nautilus_core::UnixNanos;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::DeserializeOwned};
use serde_json::value::RawValue;

/// An unavailable or malformed field never becomes a default numeric value.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub(crate) enum Field<T> {
    #[default]
    Missing,
    Null,
    Empty,
    Invalid(Box<RawValue>),
    Value(T),
}

impl<T> Field<T> {
    /// Requires a syntactically valid value, without asserting economic validity.
    pub(crate) fn require(&self, name: &str) -> anyhow::Result<&T> {
        match self {
            Self::Value(value) => Ok(value),
            Self::Missing => anyhow::bail!("Missing PAPI field {name}"),
            Self::Null => anyhow::bail!("Null PAPI field {name}"),
            Self::Empty => anyhow::bail!("Empty PAPI field {name}"),
            Self::Invalid(_) => anyhow::bail!("Invalid PAPI field {name}"),
        }
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Field<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;

        if raw.get() == "null" {
            return Ok(Self::Null);
        }

        if serde_json::from_str::<String>(raw.get()).is_ok_and(|s| s.is_empty()) {
            return Ok(Self::Empty);
        }

        Ok(match serde_json::from_str(raw.get()) {
            Ok(value) => Self::Value(value),
            Err(_) => Self::Invalid(raw),
        })
    }
}

/// An exact decimal and its original text, with no currency or balance semantics implied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Amount {
    text: String,
    value: Decimal,
}

impl Amount {
    #[must_use]
    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let unsigned = text.strip_prefix('-').unwrap_or(&text);
        let mut parts = unsigned.split('.');
        let integer = parts.next().unwrap_or_default();
        let fraction = parts.next();

        // Decimal accepts separators which are not part of the venue wire grammar
        if integer.is_empty()
            || !integer.bytes().all(|b| b.is_ascii_digit())
            || fraction.is_some_and(|s| s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()))
            || parts.next().is_some()
        {
            return Err(serde::de::Error::custom("Invalid PAPI decimal syntax"));
        }

        // Discard only fractional zero padding for parsing, retaining the original text
        let significant = if fraction.is_some() {
            text.trim_end_matches('0').trim_end_matches('.')
        } else {
            &text
        };
        let value = Decimal::from_str_exact(significant)
            .map_err(|_| serde::de::Error::custom("PAPI decimal is not exactly representable"))?;
        Ok(Self { text, value })
    }
}

impl Serialize for Amount {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text)
    }
}

/// A checked venue timestamp. Zero is retained and does not imply a fresh observation.
#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct VenueTime {
    pub(crate) milliseconds: i64,
    pub(crate) nanoseconds: UnixNanos,
}

impl<'de> Deserialize<'de> for VenueTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let milliseconds = i64::deserialize(deserializer)?;
        let nanoseconds = UnixNanos::from_millis_checked(milliseconds)
            .ok_or_else(|| serde::de::Error::custom("PAPI timestamp exceeds nanosecond bounds"))?;

        Ok(Self {
            milliseconds,
            nanoseconds,
        })
    }
}
