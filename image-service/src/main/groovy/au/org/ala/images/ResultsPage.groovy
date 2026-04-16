package au.org.ala.images

class ResultsPageList<T extends Object> implements List<T> {

    private List<T> _pageList
    private int _totalCount

    public ResultsPageList(List<T> pageList, int totalCount) {
        _pageList = pageList
        _totalCount = totalCount
    }

    public int getTotalCount() {
        return _totalCount
    }

    @Override
    int size() {
        _pageList.size()
    }

    @Override
    boolean isEmpty() {
        return _pageList.isEmpty()
    }

    @Override
    boolean contains(Object o) {
        return _pageList.contains(o)
    }

    @Override
    Iterator<T> iterator() {
        return _pageList.iterator()
    }

    @Override
    Object[] toArray() {
        return _pageList.toArray()
    }

    @Override
    def <T1> T1[] toArray(T1[] a) {
        return _pageList.toArray(a)
    }

    @Override
    boolean add(T t) {
        throw new UnsupportedOperationException()
    }

    @Override
    boolean remove(Object o) {
        throw new UnsupportedOperationException()
    }

    @Override
    boolean containsAll(Collection<?> c) {
        return _pageList.containsAll(c)
    }

    @Override
    boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException()
    }

    @Override
    boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException()
    }

    @Override
    boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException()
    }

    @Override
    boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException()
    }

    @Override
    void clear() {
        throw new UnsupportedOperationException()
    }

    @Override
    T get(int index) {
        return _pageList.get(index)
    }

    @Override
    T set(int index, T element) {
        throw new UnsupportedOperationException()
    }

    @Override
    void add(int index, T element) {
        throw new UnsupportedOperationException()
    }

    @Override
    T remove(int index) {
        throw new UnsupportedOperationException()
    }

    @Override
    int indexOf(Object o) {
        return _pageList.indexOf(o)
    }

    @Override
    int lastIndexOf(Object o) {
        return _pageList.lastIndexOf(o)
    }

    @Override
    ListIterator<T> listIterator() {
        return _pageList.listIterator()
    }

    @Override
    ListIterator<T> listIterator(int index) {
        return _pageList.listIterator(index)
    }

    @Override
    List<T> subList(int fromIndex, int toIndex) {
        return _pageList.subList(fromIndex, toIndex)
    }

}
