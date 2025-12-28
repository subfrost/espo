use crate::schemas::SchemaAlkaneId;
use borsh::{BorshDeserialize, BorshSerialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct SignedU128 {
    negative: bool,
    amount: u128,
}

impl SignedU128 {
    pub(crate) fn zero() -> Self {
        SignedU128 { negative: false, amount: 0 }
    }

    pub(crate) fn from_parts(negative: bool, amount: u128) -> Self {
        if amount == 0 { SignedU128::zero() } else { SignedU128 { negative, amount } }
    }

    pub(crate) fn positive(amount: u128) -> Self {
        SignedU128::from_parts(false, amount)
    }

    pub(crate) fn negative(amount: u128) -> Self {
        SignedU128::from_parts(true, amount)
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.amount == 0
    }

    pub(crate) fn is_negative(&self) -> bool {
        self.amount > 0 && self.negative
    }

    pub(crate) fn add(self, other: SignedU128) -> SignedU128 {
        match (self.is_negative(), other.is_negative()) {
            (false, false) => {
                SignedU128::from_parts(false, self.amount.saturating_add(other.amount))
            }
            (true, true) => SignedU128::from_parts(true, self.amount.saturating_add(other.amount)),
            (false, true) => {
                if self.amount >= other.amount {
                    SignedU128::from_parts(false, self.amount - other.amount)
                } else {
                    SignedU128::from_parts(true, other.amount - self.amount)
                }
            }
            (true, false) => {
                if self.amount >= other.amount {
                    SignedU128::from_parts(true, self.amount - other.amount)
                } else {
                    SignedU128::from_parts(false, other.amount - self.amount)
                }
            }
        }
    }

    pub(crate) fn negated(self) -> SignedU128 {
        SignedU128::from_parts(!self.is_negative(), self.amount)
    }

    pub(crate) fn as_parts(&self) -> (bool, u128) {
        (self.is_negative(), self.amount)
    }
}

impl fmt::Display for SignedU128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (is_negative, amount) = self.as_parts();
        if is_negative && amount > 0 { write!(f, "-{amount}") } else { write!(f, "{amount}") }
    }
}

impl Add for SignedU128 {
    type Output = SignedU128;

    fn add(self, rhs: SignedU128) -> SignedU128 {
        self.add(rhs)
    }
}

impl Sub for SignedU128 {
    type Output = SignedU128;

    fn sub(self, rhs: SignedU128) -> SignedU128 {
        self.add(rhs.negated())
    }
}

impl AddAssign for SignedU128 {
    fn add_assign(&mut self, rhs: SignedU128) {
        *self = self.add(rhs);
    }
}

impl SubAssign for SignedU128 {
    fn sub_assign(&mut self, rhs: SignedU128) {
        *self = self.add(rhs.negated());
    }
}

impl Neg for SignedU128 {
    type Output = SignedU128;

    fn neg(self) -> SignedU128 {
        self.negated()
    }
}

impl PartialOrd for SignedU128 {
    fn partial_cmp(&self, other: &SignedU128) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SignedU128 {
    fn cmp(&self, other: &SignedU128) -> Ordering {
        match (self.is_negative(), other.is_negative()) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.amount.cmp(&other.amount),
            (true, true) => other.amount.cmp(&self.amount),
        }
    }
}

pub(crate) trait SignedU128MapExt {
    fn add_signed(&mut self, key: SchemaAlkaneId, delta: SignedU128);
}

impl SignedU128MapExt for BTreeMap<SchemaAlkaneId, SignedU128> {
    fn add_signed(&mut self, key: SchemaAlkaneId, delta: SignedU128) {
        if delta.is_zero() {
            return;
        }
        let entry = self.entry(key).or_insert_with(SignedU128::zero);
        *entry += delta;
        if entry.is_zero() {
            self.remove(&key);
        }
    }
}

#[allow(dead_code)]
pub(crate) trait SignedU128Math {
    fn try_add_signed(self, delta: SignedU128) -> Option<u128>;
    fn try_sub_signed(self, delta: SignedU128) -> Option<u128>;
}

impl SignedU128Math for u128 {
    fn try_add_signed(self, delta: SignedU128) -> Option<u128> {
        let (is_negative, amount) = delta.as_parts();
        if is_negative { self.checked_sub(amount) } else { self.checked_add(amount) }
    }

    fn try_sub_signed(self, delta: SignedU128) -> Option<u128> {
        self.try_add_signed(delta.negated())
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EspoTraceType {
    NOTRACE,
    REVERT,
    SUCCESS,
}
