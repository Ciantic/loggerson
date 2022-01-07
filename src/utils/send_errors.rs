use rayon::iter::ParallelIterator;

// TODO: Simplyfy the generics, they are overqualifying

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct SendErrors<'s, I, M> {
    iter: I,
    channel: &'s crossbeam_channel::Sender<M>,
}

impl<'s, I, E, T, M> SendErrors<'s, I, M>
where
    I: Iterator<Item = Result<T, E>>,
    M: From<E>,
{
    pub(self) fn new(iter: I, channel: &'s crossbeam_channel::Sender<M>) -> SendErrors<I, M> {
        SendErrors { iter, channel }
    }
}

impl<'s, I, E, T, M> Iterator for SendErrors<'s, I, M>
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
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> SendErrors<T, M>;
}

impl<T, V, E, M> SendErrorsExt<T, V, E, M> for T
where
    T: Iterator<Item = Result<V, E>>,
    M: From<E>,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors(self, channel: &crossbeam_channel::Sender<M>) -> SendErrors<T, M> {
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

/*
// Adds the `send_error_as` to the ParallelIterator
pub trait ParallelSendErrorsAsExt<T, V, E, M, E2, F>
where
    T: ParallelIterator<Item = Result<V, E2>>,
    M: From<E2>,
    F: Fn(E) -> E2 + Send + Sync,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors_as(
        self,
        channel: &crossbeam_channel::Sender<M>,
        f: F,
    ) -> ParallelSendErrors<T, E2, V, M>;
}

impl<I, T, V, E, M, E2, F> ParallelSendErrorsAsExt<T, V, E, M, E2, F> for I
where
    I: ParallelIterator<Item = Result<V, E>>,
    T: ParallelIterator<Item = Result<V, E2>>,
    M: From<E2>,
    V: Send + Sync,
    E2: Send + Sync,
    F: Fn(E) -> E2 + Send + Sync,
{
    /// Transmit errors to a channel, leaving Ok values in the iterator
    fn send_errors_as(
        self,
        channel: &crossbeam_channel::Sender<M>,
        f: F,
    ) -> ParallelSendErrors<T, E2, V, M> {
        let iter: T = self.map(|res| res.map_err(f)).into_par_iter();
        ParallelSendErrors::new(iter, channel.clone())
    }
}
 */
