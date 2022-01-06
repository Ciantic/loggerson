use rayon::iter::ParallelIterator;

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct SendErrors<'s, I, E, T, M>
where
    I: Iterator<Item = Result<T, E>>,
    M: From<E>,
{
    iter: I,
    channel: &'s crossbeam_channel::Sender<M>,
}

impl<'s, I, E, T, M> SendErrors<'s, I, E, T, M>
where
    I: Iterator<Item = Result<T, E>>,
    M: From<E>,
{
    pub(crate) fn new(
        iter: I,
        channel: &'s crossbeam_channel::Sender<M>,
    ) -> SendErrors<I, E, T, M> {
        SendErrors { iter, channel }
    }
}

impl<'s, I, E, T, M> Iterator for SendErrors<'s, I, E, T, M>
where
    I: Iterator<Item = Result<T, E>>,
    M: From<E>,
{
    #[inline]
    fn next(&mut self) -> Option<T> {
        loop {
            match self.iter.next() {
                Some(Ok(v)) => return Some(v),
                Some(Err(v)) => {
                    self.channel.send(M::from(v)).unwrap();
                    continue;
                }
                None => return None,
            }
        }
    }

    type Item = T;
}

// Adds the `transmit_error` to the Iterator
pub trait SendErrorsExt<T, V, E, M>
where
    T: Iterator<Item = Result<V, E>>,
    M: From<E>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> SendErrors<T, E, V, M>;
}

impl<T, V, E, M> SendErrorsExt<T, V, E, M> for T
where
    T: Iterator<Item = Result<V, E>>,
    M: From<E>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> SendErrors<T, E, V, M> {
        SendErrors::new(self, channel)
    }
}

/* -------------------------------------------------------------------- */

// Transmit errors (parallel version)

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct ParallelSendErrors<I, E, T, M>
where
    I: ParallelIterator<Item = Result<T, E>>,
    M: From<E>,
{
    iter: I,
    channel: crossbeam_channel::Sender<M>,
}

impl<I, E, T, M> ParallelSendErrors<I, E, T, M>
where
    I: ParallelIterator<Item = Result<T, E>>,
    M: From<E>,
{
    pub(crate) fn new(
        iter: I,
        channel: crossbeam_channel::Sender<M>,
    ) -> ParallelSendErrors<I, E, T, M> {
        ParallelSendErrors { iter, channel }
    }
}

impl<I, E, T, M> ParallelIterator for ParallelSendErrors<I, E, T, M>
where
    I: ParallelIterator<Item = Result<T, E>>,
    M: From<E> + Send,
    T: Send,
{
    type Item = T;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        self.iter
            .filter_map(|v| match v {
                Ok(v) => Some(v),
                Err(v) => {
                    self.channel.send(M::from(v)).unwrap();
                    None
                }
            })
            .drive_unindexed(consumer)
    }
}

// Adds the `transmit_error` to the ParallelIterator
pub trait ParallelSendErrorsExt<T, V, E, M>
where
    T: ParallelIterator<Item = Result<V, E>>,
    M: From<E>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> ParallelSendErrors<T, E, V, M>;
}

impl<T, V, E, M> ParallelSendErrorsExt<T, V, E, M> for T
where
    T: ParallelIterator<Item = Result<V, E>>,
    M: From<E>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> ParallelSendErrors<T, E, V, M> {
        ParallelSendErrors::new(self, channel.clone())
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
