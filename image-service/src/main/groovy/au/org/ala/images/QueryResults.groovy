package au.org.ala.images

/**
 * For when you need to return both a page worth of results and the total count of record (for pagination purposes)
 *
 * @param < T > Usually a domain class. The type of objects being returned in the list
 */
class QueryResults <T> {

    public List<T> list = []
    public long totalCount = 0
    public Map<String, Map<String, Object>> aggregations = [:]
    public Map<Object,Object> filters = [:]
}
