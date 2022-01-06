use std::{iter::FilterMap, marker::PhantomData};

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct TransmitErrors<'s, I, E, T>
where
    I: Iterator<Item = Result<T, E>>,
{
    iter: I,
    channel: &'s std::sync::mpsc::Sender<E>,
}

impl<'s, I, E, T> TransmitErrors<'s, I, E, T>
where
    I: Iterator<Item = Result<T, E>>,
{
    pub(crate) fn new(iter: I, channel: &'s std::sync::mpsc::Sender<E>) -> TransmitErrors<I, E, T> {
        TransmitErrors { iter, channel }
    }
}

impl<'s, I, E, T> Iterator for TransmitErrors<'s, I, E, T>
where
    I: Iterator<Item = Result<T, E>>,
{
    #[inline]
    fn next(&mut self) -> Option<T> {
        loop {
            match self.iter.next() {
                Some(Ok(v)) => return Some(v),
                Some(Err(v)) => {
                    self.channel.send(v).unwrap();
                    continue;
                }
                None => return None,
            }
        }
    }

    type Item = T;
}

// Adds the `transmit_error` to the Iterator
pub trait TransmitErrorsExt<T, V, E>
where
    T: Iterator<Item = Result<V, E>>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn transmit_errors(self, channel: &std::sync::mpsc::Sender<E>) -> TransmitErrors<T, E, V>;
}

impl<T, V, E> TransmitErrorsExt<T, V, E> for T
where
    T: Iterator<Item = Result<V, E>>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn transmit_errors(self, channel: &std::sync::mpsc::Sender<E>) -> TransmitErrors<T, E, V> {
        TransmitErrors::new(self, channel)
    }
}

/* -------------------------------------------------------------------- */

pub trait ExtendTo<T, I, R>
where
    R: Extend<T>,
    I: IntoIterator<Item = T>,
{
    /// Moves the values to mutable reference which implements `extend()`
    fn extend_to(self, mutref: &mut R) -> ();
}

impl<T, I, R> ExtendTo<T, I, R> for I
where
    R: Extend<T>,
    I: IntoIterator<Item = T>,
{
    /// Moves the values to mutable reference which implements `extend()`
    fn extend_to(self, mutref: &mut R) {
        mutref.extend(self);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::ExtendTo;

    #[test]
    fn extend_to_vec() {
        let input = vec![4, 5];
        let mut receiver = vec![1, 2, 3];
        input.iter().extend_to(&mut receiver);
        assert_eq!(vec![1, 2, 3, 4, 5], receiver);
    }

    #[test]
    fn extend_to_hashmap() {
        let input = vec![("a", 1), ("b", 2)];
        let mut receiver: HashMap<_, _> = vec![("c", 3), ("d", 4)].into_iter().collect();

        // Notice: into_iter is required
        input.into_iter().extend_to(&mut receiver);
        let mut gotem = receiver.into_iter().collect::<Vec<_>>();
        gotem.sort();
        assert_eq!(vec![("a", 1), ("b", 2), ("c", 3), ("d", 4)], gotem);
    }
}
