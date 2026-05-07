package au.org.ala.images

import java.text.SimpleDateFormat
import java.util.regex.Pattern

public class SearchCriteriaUtils {

    public static void buildCriteria(Object delegate, List<SearchCriteria> criteriaList) {
        criteriaList.each {
            def translator = factory(it)
            translator.injectCriteria(it, delegate)
        }
    }

    public static String format(SearchCriteria criteria, Closure<String> valueFormatter = null) {
        CriteriaTranslator translator = factory(criteria)
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

    public static CriteriaTranslator factory(SearchCriteria criteria) {
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

    public static class MultiStringPatternTranslator implements CriteriaTranslator {
        String[] values

        public MultiStringPatternTranslator(String value) {
            this.values = value?.split("\\|")
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
        public void injectCriteria(SearchCriteria criteria, Object delegate) {
            def field = criteria.criteriaDefinition.fieldName
            if (values.size() == 1) {
                delegate."ilike"(field, "${values[0]}")
            } else {
                delegate."in"(field, values)
            }
        }

    }

    public static class DoubleCriteriaTranslator implements CriteriaTranslator {

        public static Pattern DoublePattern = Pattern.compile("^(eq|gt|lt)\\s([-]{0,1}\\d+[\\.]{0,1}\\d*)\$")
        public static Pattern DoubleRangePattern = Pattern.compile("^(bt)\\s([-]{0,1}\\d+[\\.]{0,1}\\d*)[:]([-]{0,1}\\d+[\\.]{0,1}\\d*)\$")

        String operator
        Double value1
        Double value2

        public DoubleCriteriaTranslator(String pattern) {
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
        public void injectCriteria(SearchCriteria criteria, Object delegate) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "eq":
                    delegate."eq"(field, value1)
                    break;
                case "lt":
                    delegate."le"(field, value1)
                    break
                case "gt":
                    delegate."ge"(field, value1)
                    break
                case "bt":
                    delegate."between"(field, value1, value2)
                    break
            }
        }

        public String displayString(Closure<String> formatValue) {
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

    public static class IntegerCriteriaTranslator extends NumberCriteriaTranslator<Integer> {

        IntegerCriteriaTranslator(String pattern) {
            super(pattern)
        }

        @Override
        protected Integer parseString(String s) {
            return s.toInteger()
        }

    }

    public static class LongCriteriaTranslator extends NumberCriteriaTranslator<Long> {

        LongCriteriaTranslator(String pattern) {
            super(pattern)
        }

        @Override
        protected Long parseString(String s) {
            return s.toLong()
        }

    }

    private abstract static class NumberCriteriaTranslator<T extends Number> implements CriteriaTranslator {

        public static Pattern IntegerPattern = Pattern.compile("^(eq|gt|lt)\\s([-]{0,1}\\d+)\$")
        public static Pattern IntegerRangePattern = Pattern.compile("^(bt)\\s([-]{0,1}\\d+)[:]([-]{0,1}\\d+)\$")
        public static Pattern IntegerFieldPattern = Pattern.compile("^(eq|gt|lt)\\sfield[(](\\w+)[)]\$")

        String operator
        T value1
        T value2
        String otherField

        protected abstract T parseString(String s);

        public NumberCriteriaTranslator(String pattern) {
            def m = IntegerPattern.matcher(pattern)

            otherField = null

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
                    m = IntegerFieldPattern.matcher(pattern)
                    if (m.matches()) {
                        operator = m.group(1)
                        otherField = m.group(2)
                        value1 = null
                        value2 = null
                    } else {
                        throw new RuntimeException("Unrecognized number range criteria format: " + pattern)
                    }
                }
            }
        }

        public String displayString(Closure<String> formatValue) {
            def value = value1
            if (otherField) {
                value = otherField
            }

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
        public void injectCriteria(SearchCriteria criteria, Object delegate) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "eq":
                    if (otherField) {
                        delegate."eqProperty"(field, otherField)
                    } else {
                        delegate."eq"(field, value1)
                    }
                    break;
                case "lt":
                    if (otherField) {
                        delegate."leProperty"(field, otherField)
                    } else {
                        delegate."le"(field, value1)
                    }
                    break
                case "gt":
                    if (otherField) {
                        delegate."gtProperty"(field, otherField)
                    } else {
                        delegate."gt"(field, value1)
                    }
                    break
                case "bt":
                    delegate."between"(field, value1, value2)
                    break
            }
        }
    }

    public static class DateRangeCriteriaTranslator implements  CriteriaTranslator {

        public static Pattern DatePattern = Pattern.compile("^(gt|lt)\\s(\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)\$")
        public static Pattern DateRangePattern = Pattern.compile("^(bt)\\s(\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)[:](\\d{1,2}/\\d{1,2}/\\d\\d\\d\\d)\$")

        def _sdf = new SimpleDateFormat("dd/MM/yyyy")
        // def _sdf2 = new SimpleDateFormat("MMM dd, yyyy")

        Date startDate
        Date endDate
        String operator

        public DateRangeCriteriaTranslator(String pattern) {
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
        void injectCriteria(SearchCriteria criteria, Object delegate) {
            def field = criteria.criteriaDefinition.fieldName
            switch (operator) {
                case "lt":
                    delegate."le"(field, startDate)
                    break
                case "gt":
                    delegate."ge"(field, startDate)
                    break
                case "bt":
                    delegate."between"(field, startDate, endDate)
                    break
            }
        }

    }

    public interface CriteriaTranslator {
        public String displayString(Closure<String> valueFormatter)
        public void injectCriteria(SearchCriteria criteria, Object delegate)
    }

}