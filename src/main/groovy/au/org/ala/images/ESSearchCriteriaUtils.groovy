package au.org.ala.images

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders
import co.elastic.clients.elasticsearch._types.query_dsl.QueryVariant
import java.text.SimpleDateFormat
import java.util.regex.Pattern

class ESSearchCriteriaUtils {

    static void buildCriteria(BoolQuery.Builder boolQueryBuilder, List<SearchCriteria> criteriaList) {
        criteriaList.each {
            def translator = factory(it)
            boolQueryBuilder.must(translator.createQueryBuilder(it)._toQuery())
        }
    }

    static String format(SearchCriteria criteria, Closure<String> valueFormatter = null) {
        ESCriteriaTranslator translator = factory(criteria)
        if (translator) {
            if (!valueFormatter) {
                valueFormatter = { it }
            }
            def output = translator.displayString(valueFormatter)
            if (criteria.criteriaDefinition.units) {
                output += " (" + criteria.criteriaDefinition.units + ")"
            }

            return output
        }
        return criteria.value
    }

    static ESCriteriaTranslator factory(SearchCriteria criteria) {
        switch (criteria.criteriaDefinition.valueType) {
            case CriteriaValueType.NumberRangeDouble:
                return new DoubleCriteriaTranslator(criteria.value)
                break
            case CriteriaValueType.NumberRangeInteger:
                return new IntegerCriteriaTranslator(criteria.value)
                break
            case CriteriaValueType.NumberRangeLong:
                return new LongCriteriaTranslator(criteria.value)
                break
            case CriteriaValueType.StringDirectEntry:
            case CriteriaValueType.StringMultiSelect:
                return new MultiStringPatternTranslator(criteria.value)
                break
            case CriteriaValueType.StringSingleSelect:
                //return new StringPatternTranslator(criteria.value)
                break
            case CriteriaValueType.DateRange:
                return new DateRangeCriteriaTranslator(criteria.value)
                break
            case CriteriaValueType.Boolean:
                //return new BooleanCriteriaTranslator(criteria.value)
                break
            default:
                throw new RuntimeException("Unhandled value type: " + criteria.criteriaDefinition.valueType)
        }
    }

    static class MultiStringPatternTranslator implements ESCriteriaTranslator {
        String[] values

        MultiStringPatternTranslator(String value) {
            this.values = value?.split("~")
        }

        String displayString(Closure<String> formatValue) {
            if (values) {
                def sb = new StringBuilder()
                if (values.size() == 1) {
                    sb << "matches " + formatValue(values[0])
                } else {
                    sb << "is one of "
                    def formattedValues = values.collect { formatValue(it) }
                    sb << formattedValues[0..formattedValues.size() - 2].join(", ")
                    sb << " or "
                    sb << formattedValues[formattedValues.size()-1]
                }
                return sb.toString()
            }
            return "[Empty list!]"
        }

        @Override
        QueryVariant createQueryBuilder(SearchCriteria criteria) {

            def escape = { String term ->
                return term.replaceAll("/", "\\\\/").replaceAll(":", "\\\\:")
            }
            def field = criteria.criteriaDefinition.fieldName

            if (values.size() == 1) {
                return QueryBuilders.queryString().query("${field}:${escape(values[0])}").build()
            } else {
                def fb = QueryBuilders.bool().filter(
                        values.collect {
                            QueryBuilders.queryString().query("${field}:${escape(it)}").build()
                        }
                )
                return fb.build()
            }
        }

    }

    static class DoubleCriteriaTranslator implements ESCriteriaTranslator {

        public static Pattern DoublePattern = Pattern.compile("^(eq|gt|lt)\\s([-]{0,1}\\d+[\\.]{0,1}\\d*)\$")
        public static Pattern DoubleRangePattern = Pattern.compile("^(bt)\\s([-]{0,1}\\d+[\\.]{0,1}\\d*)[:]([-]{0,1}\\d+[\\.]{0,1}\\d*)\$")

        String operator
        Double value1
        Double value2

        DoubleCriteriaTranslator(String pattern) {
            def m = DoublePattern.matcher(pattern)
            if (m.matches()) {
                operator = m.group(1)
                value1 = Double.parseDouble(m.group(2))
            } else {
                m = DoubleRangePattern.matcher(pattern)
                if (m.matches()) {
                    operator = m.group(1)
                    def num1 = Double.parseDouble(m.group(2))
                    def num2 = Double.parseDouble(m.group(3))
                    value1 = Math.min(num1, num2)
                    value2 = Math.max(num1, num2)
                } else {
                    throw new RuntimeException("Unrecognized number range criteria format: " + pattern)
                }
            }

        }

        @Override
        QueryVariant createQueryBuilder(SearchCriteria criteria) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "eq":
                    return QueryBuilders.bool().filter(QueryBuilders.queryString().query("${field}:${value1}").build()).build()
//                    return QueryBuilders.queryFilter(QueryBuilders.queryString("${field}:value1"))
                case "lt":
                    return QueryBuilders.bool().filter(QueryBuilders.range().field(field).lte(JsonData.of(value1)).build()).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(new NumberRangeQuery.Builder().field(field).lte(value1).build()).build()
//                    return QueryBuilders.rangeFilter(field).lte(value1)
                case "gt":
                    return QueryBuilders.bool().filter(QueryBuilders.range().field(field).gte(JsonData.of(value1)).build()).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(new NumberRangeQuery.Builder().field(field).gte(value1).build()).build()
//                    return QueryBuilders.rangeFilter(field).gte(value1)
                case "bt":
                    return QueryBuilders.bool().filter(QueryBuilders.range().field(field).gte(JsonData.of(value1)).lte(JsonData.of(value2)).build()).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(new NumberRangeQuery.Builder().field(field).gte(value1).lte(value2).build()).build()
//                    return QueryBuilders.rangeFilter(field).gte(value1).lte(value2)
                default:
                    throw new RuntimeException("Unrecognized number range criteria operator: " + operator)
            }
        }

        String displayString(Closure<String> formatValue) {
            switch (operator) {
                case "eq":
                    return "is equal to " + formatValue(value1)
                case "lt":
                    return "is less or equal to " + formatValue(value1)
                    break
                case "gt":
                    return "is greater or equal to " + formatValue(value1)
                    break
                case "bt":
                    return "is between ${formatValue(value1)} and ${formatValue(value2)}"
                    break
            }
            return "???"
        }

    }

    static class IntegerCriteriaTranslator extends ESNumberCriteriaTranslator<Integer> {

        IntegerCriteriaTranslator(String pattern) {
            super(pattern)
        }

        @Override
        protected Integer parseString(String s) {
            return s.toInteger()
        }

    }

    static class LongCriteriaTranslator extends ESNumberCriteriaTranslator<Long> {

        LongCriteriaTranslator(String pattern) {
            super(pattern)
        }

        @Override
        protected Long parseString(String s) {
            return s.toLong()
        }

    }

    private abstract static class ESNumberCriteriaTranslator<T extends Number> implements ESCriteriaTranslator {

        public static Pattern IntegerPattern = Pattern.compile("^(eq|gt|lt)\\s([-]{0,1}\\d+)\$")
        public static Pattern IntegerRangePattern = Pattern.compile("^(bt)\\s([-]{0,1}\\d+)[:]([-]{0,1}\\d+)\$")

        String operator
        T value1
        T value2

        protected abstract T parseString(String s);

        ESNumberCriteriaTranslator(String pattern) {
            def m = IntegerPattern.matcher(pattern)

            if (m.matches()) {
                operator = m.group(1)
                value1 = parseString(m.group(2))
            } else {
                m = IntegerRangePattern.matcher(pattern)
                if (m.matches()) {
                    operator = m.group(1)
                    def num1 = parseString(m.group(2))
                    def num2 = parseString(m.group(3))
                    value1 = Math.min(num1, num2)
                    value2 = Math.max(num1, num2)
                } else {
                    throw new RuntimeException("Unrecognized number range criteria format: " + pattern)
                }
            }
        }

        String displayString(Closure<String> formatValue) {
            def value = value1

            switch (operator) {
                case "eq":
                    return "is equal to " + formatValue(value)
                case "lt":
                    return "is less or equal to " + formatValue(value)
                    break
                case "gt":
                    return "is greater or equal to " + formatValue(value)
                    break
                case "bt":
                    return "is between ${formatValue(value1)} and ${formatValue(value2)}"
                    break
            }
            return "???"
        }

        @Override
        QueryVariant createQueryBuilder(SearchCriteria criteria) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "eq":
                    return QueryBuilders.queryString().query("${field}:${value1}").build()
                case "lt":
                    return QueryBuilders.range().field(field).lte(JsonData.of(value1)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(RangeQueryBuilders.number().field(field).lte(value1).build()).build()
                case "gt":
                    return QueryBuilders.range().field(field).gte(JsonData.of(value1)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(RangeQueryBuilders.number().field(field).gte(value1).build()).build()
                case "bt":
                    return QueryBuilders.range().field(field).gte(JsonData.of(value1)).lte(JsonData.of(value2)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().number(RangeQueryBuilders.number().field(field).gte(value1).lte(value2).build()).build()
                default:
                    throw new RuntimeException("Unrecognized number range criteria operator: " + operator)
            }
        }
    }

    static class DateRangeCriteriaTranslator implements  ESCriteriaTranslator {

        public static Pattern DatePattern = Pattern.compile("^(gt|lt)\\s(\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)\$")
        public static Pattern DateRangePattern = Pattern.compile("^(bt)\\s(\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)[:](\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)\$")

        def _sdf = new SimpleDateFormat("dd/MM/yyyy")
        // def _sdf2 = new SimpleDateFormat("MMM dd, yyyy")

        Date startDate
        Date endDate
        String operator

        DateRangeCriteriaTranslator(String pattern) {
            def m = DatePattern.matcher(pattern)
            if (m.matches()) {
                operator = m.group(1)
                startDate = _sdf.parse(m.group(2))
            } else {
                m = DateRangePattern.matcher(pattern)
                if (m.matches()) {
                    operator = m.group(1)
                    def date1 = _sdf.parse(m.group(2))
                    def date2 = _sdf.parse(m.group(3))
                    startDate = [date1, date2].min()
                    endDate = [date1, date2].max()
                } else {
                    throw new RuntimeException("Unrecognized date range criteria format: " + pattern)
                }
            }

        }


        @Override
        String displayString(Closure<String> valueFormatter) {
            switch (operator) {
                case "lt":
                    return "is before " + valueFormatter(_sdf.format(startDate))
                    break
                case "gt":
                    return "is after " + valueFormatter(_sdf.format(startDate))
                    break
                case "bt":
                    return "is between " + valueFormatter(_sdf.format(startDate)) + " and " + valueFormatter(_sdf.format(endDate))
                    break
            }
        }

        @Override
        QueryVariant createQueryBuilder(SearchCriteria criteria) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "lt":
                    return QueryBuilders.range().field(field).lte(JsonData.of(startDate)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().date(RangeQueryBuilders.date().field(field).lte(startDate).build()).build()
//                    return QueryBuilders.rangeFilter(field).lte(startDate)
                case "gt":
                    return QueryBuilders.range().field(field).gte(JsonData.of(startDate)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().date(RangeQueryBuilders.date().field(field).gte(startDate).build()).build()
//                    return QueryBuilders.rangeFilter(field).gte(startDate)
                case "bt":
                    return QueryBuilders.range().field(field).gte(JsonData.of(startDate)).lte(JsonData.of(endDate)).build()
                    // TODO ES 8.0+ uses a different range query builder
//                    return QueryBuilders.range().date(RangeQueryBuilders.date().field(field).gte(startDate).lte(endDate).build()).build()
//                    return QueryBuilders.rangeFilter(field).gte(startDate).lte(endDate)
                default:
                    throw new RuntimeException("Unrecognized date range criteria operator: " + operator)
            }
        }

    }

    interface ESCriteriaTranslator {
        String displayString(Closure<String> valueFormatter)
        QueryVariant createQueryBuilder(SearchCriteria criteria)
    }

}