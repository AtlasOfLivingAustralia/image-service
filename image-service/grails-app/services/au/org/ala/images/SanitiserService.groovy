package au.org.ala.images

import groovy.util.logging.Slf4j
import org.apache.commons.lang3.StringUtils
import org.owasp.html.HtmlChangeListener
import org.owasp.html.HtmlPolicyBuilder
import org.owasp.html.HtmlStreamEventProcessor
import org.owasp.html.HtmlStreamEventReceiver
import org.owasp.html.HtmlStreamEventReceiverWrapper
import org.owasp.html.PolicyFactory
import org.owasp.html.Sanitizers

import javax.annotation.Nullable

@Slf4j
class SanitiserService {

    /** Allow simple formatting, links and text within p and divs by default */
    def policy = (Sanitizers.FORMATTING & Sanitizers.LINKS) & new HtmlPolicyBuilder().allowTextIn("p", "div").toFactory()


    /**
     * Sanitise an input string with the default policy and no warnings
     * @param input
     * @return
     */
    String sanitise(String input) {
        internalSanitise(policy, input)
    }

    String truncateAndSanitise(String input, int length) {
        internalSanitise(policy & truncater(length), input)
    }



    /**
     * Sanitise an image property, providiing the imager and property name as context
     * so that the offending data can be logged.
     *
     * @param input The input string
     * @param image The image the input string belongs to
     * @param propertyName The image property name the input string belongs to
     * @return
     */
    String sanitise(String input, String imageId, String propertyName) {
        internalSanitise(policy, input, imageId, propertyName)
    }

    String truncateAndSanitise(String input, String imageId, String propertyName, int length) {
        def truncatingPolicy = policy & truncater(length)
        internalSanitise(truncatingPolicy, input, imageId, propertyName)
    }

    private static PolicyFactory truncater(int length) {
        new HtmlPolicyBuilder().withPostprocessor(new HtmlStreamEventProcessor() {
            @Override
            HtmlStreamEventReceiver wrap(HtmlStreamEventReceiver sink) {
                return new TextTruncater(sink, length)
            }
        }).toFactory()
    }

    private static String internalSanitise(PolicyFactory policyFactory, String input, String imageId = '', String metadataName = '') {
        policyFactory.sanitize(input, new HtmlChangeListener<Object>() {
            void discardedTag(@Nullable Object context, String elementName) {
                SanitiserService.log.warn("Dropping element $elementName in $imageId.$metadataName")
            }
            void discardedAttributes(@Nullable Object context, String tagName, String... attributeNames) {
                SanitiserService.log.warn("Dropping attributes $attributeNames from $tagName in $imageId.$metadataName")
            }
        }, null)
    }

    /**
     * Allows up to length characters of text from the underlying HTML to be sent to the output, truncating the text if
     * it's longer than length and omitting any subsequent tags.
     */
    static class TextTruncater extends HtmlStreamEventReceiverWrapper {

        private final int length
        private int spent = 0
        private int closeNextTag = 0
        private int level = 0

        TextTruncater(HtmlStreamEventReceiver underlying, int length) {
            super(underlying)
            this.length = length
        }

        @Override
        void text(String text) {
            int newLength = text.length()
            if (spent > length) {
                // already spent our text budget, do nothing
            } else if (spent + newLength > length) {
                if (length - spent < 4) {
                    this.underlying.text('...')
                } else {
                    this.underlying.text(StringUtils.abbreviate(text, length - spent))
                }
                closeNextTag = level
                spent += newLength
            } else {
                this.underlying.text(text)
                spent += newLength
            }
        }

        @Override
        void openTag(String elementName, List<String> attrs) {
            level++
            if (spent > length) {
                // already spent our length budget, omit new tags
            } else {
                super.openTag(elementName, attrs)
            }

        }

        @Override
        void closeTag(String elementName) {
            if (spent <= length) {
                super.closeTag(elementName)
            } else if (closeNextTag == level) {
                // need to close this tag as it was the last open tag before the length budget was exhausted
                closeNextTag--
                super.closeTag(elementName)
            }
            level--
        }
    }
}
