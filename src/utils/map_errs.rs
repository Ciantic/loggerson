use rayon::iter::ParallelIterator;

// TODO: Simplyfy the generics, they are overqualifying

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct MapErrs<I, F> {
    iter: I,
    map_op: F,
}

impl<I, F, V, E, E2> MapErrs<I, F>
where
    I: Iterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    pub(crate) fn new(iter: I, map_op: F) -> MapErrs<I, F> {
        MapErrs { iter, map_op }
    }
}

impl<I, F, V, E, E2> Iterator for MapErrs<I, F>
where
    I: Iterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
    V: Send,
    E2: Send,
{
    type Item = Result<V, E2>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                Some(Ok(v)) => return Some(Ok(v)),
                Some(Err(v)) => return Some(Err((self.map_op)(v))),
                None => return None,
            }
        }
    }
}

pub trait MapErrsExt<I, V, E, E2, F>
where
    I: Iterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    fn map_errs(self, f: F) -> MapErrs<I, F>;
}

impl<I, V, E, E2, F> MapErrsExt<I, V, E, E2, F> for I
where
    I: Iterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    fn map_errs(self, f: F) -> MapErrs<I, F> {
        MapErrs::new(self, f)
    }
}

/* ----------------------------------------------------------------------- */

// TODO: Simplyfy the generics, they are overqualifying

#[must_use = "iterators are lazy and do nothing unless consumed"]
#[derive(Clone)]
pub struct ParallelMapErrs<I, F> {
    iter: I,
    map_op: F,
}

impl<I, F, V, E, E2> ParallelMapErrs<I, F>
where
    I: ParallelIterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    pub(crate) fn new(iter: I, map_op: F) -> ParallelMapErrs<I, F> {
        ParallelMapErrs { iter, map_op }
    }
}

impl<I, F, V, E, E2> ParallelIterator for ParallelMapErrs<I, F>
where
    I: ParallelIterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
    V: Send,
    E2: Send,
{
    type Item = Result<V, E2>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: rayon::iter::plumbing::UnindexedConsumer<Self::Item>,
    {
        self.iter
            .map(|v| match v {
                Ok(v) => Ok(v),
                Err(er) => Err((self.map_op)(er)),
            })
            .drive_unindexed(consumer)
    }
}

pub trait ParallelMapErrsExt<I, V, E, E2, F>
where
    I: ParallelIterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    fn map_errs(self, f: F) -> ParallelMapErrs<I, F>;
}

impl<I, V, E, E2, F> ParallelMapErrsExt<I, V, E, E2, F> for I
where
    I: ParallelIterator<Item = Result<V, E>>,
    F: Fn(E) -> E2 + Sync + Send,
{
    fn map_errs(self, f: F) -> ParallelMapErrs<I, F> {
        ParallelMapErrs::new(self, f)
    }
}
