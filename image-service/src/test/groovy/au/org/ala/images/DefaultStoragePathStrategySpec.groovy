package au.org.ala.images

import spock.lang.Specification
import spock.lang.Unroll

class DefaultStoragePathStrategySpec extends Specification {

    def setup() {

    }

    @Unroll
    def 'test storage path strategy basePath #prefix #unixSep #absPath'(prefix, unixSep, absPath, result) {
        setup:
        def sps = new DefaultStoragePathStrategy(prefix, unixSep, absPath)

        when:
        def basePath = sps.basePath()

        then:
        basePath == result

        where:
        prefix || unixSep || absPath || result
        '/'     | true     | true     | '/'
        '/'     | true     | false     | ''
        '//'     | true     | true     | '/'
        '//'     | true     | false     | ''
        ''     | true     | true     | '/'
        ''     | true     | false     | ''
        '/asdf'     | true     | true     | '/asdf'
        '/asdf'     | true     | false     | 'asdf'
        'asdf'     | true     | true     | '/asdf'
        'asdf'     | true     | false     | 'asdf'
        '/asdf/asdf'     | true     | true     | '/asdf/asdf'
        '/asdf/asdf'     | true     | false     | 'asdf/asdf'
        'asdf/asdf'     | true     | true     | '/asdf/asdf'
        'asdf/asdf'     | true     | false     | 'asdf/asdf'
        '//asdf//asdf/'     | true     | true     | '/asdf/asdf'
        '//asdf//asdf/'     | true     | false     | 'asdf/asdf'
        'asdf//asdf/'     | true     | true     | '/asdf/asdf'
        'asdf//asdf/'     | true     | false     | 'asdf/asdf'
    }

    @Unroll
    def 'test storage path strategy original #prefix #unixSep #absPath'(prefix, uuid, unixSep, absPath, result) {
        setup:
        def sps = new DefaultStoragePathStrategy(prefix, unixSep, absPath)

        when:
        def origPath = sps.createOriginalPathFromUUID(uuid)

        then:
        origPath == result

        where:
        prefix || uuid || unixSep || absPath || result
        '/'     | 'qwer' | true     | true     | '/r/e/w/q/qwer/original'
        '/'     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/original'
        '//'     | 'qwer' |  true     | true     | '/r/e/w/q/qwer/original'
        '//'     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/original'
        ''     | 'qwer' |  true     | true     | '/r/e/w/q/qwer/original'
        ''     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/original'
        '/asdf'     | 'qwer' |  true     | true     | '/asdf/r/e/w/q/qwer/original'
        '/asdf'     | 'qwer' |  true     | false     | 'asdf/r/e/w/q/qwer/original'
        'asdf'     | 'qwer' |  true     | true     | '/asdf/r/e/w/q/qwer/original'
        'asdf'     | 'qwer' |  true     | false     | 'asdf/r/e/w/q/qwer/original'
        '/asdf/asdf'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/original'
        '/asdf/asdf'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/original'
        'asdf/asdf'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/original'
        'asdf/asdf'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/original'
        '//asdf//asdf/'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/original'
        '//asdf//asdf/'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/original'
        'asdf//asdf/'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/original'
        'asdf//asdf/'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/original'
    }

    @Unroll
    def 'test storage path strategy tiles #prefix #unixSep #absPath'(prefix, uuid, unixSep, absPath, result) {
        setup:
        def sps = new DefaultStoragePathStrategy(prefix, unixSep, absPath)

        when:
        def origPath = sps.createTilesPathFromUUID(uuid, 1, 2, 3)

        then:
        origPath == result

        where:
        prefix || uuid || unixSep || absPath || result
        '/'     | 'qwer' | true     | true     | '/r/e/w/q/qwer/tms/3/1/2.png'
        '/'     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/tms/3/1/2.png'
        '//'     | 'qwer' |  true     | true     | '/r/e/w/q/qwer/tms/3/1/2.png'
        '//'     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/tms/3/1/2.png'
        ''     | 'qwer' |  true     | true     | '/r/e/w/q/qwer/tms/3/1/2.png'
        ''     | 'qwer' |  true     | false     | 'r/e/w/q/qwer/tms/3/1/2.png'
        '/asdf'     | 'qwer' |  true     | true     | '/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        '/asdf'     | 'qwer' |  true     | false     | 'asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf'     | 'qwer' |  true     | true     | '/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf'     | 'qwer' |  true     | false     | 'asdf/r/e/w/q/qwer/tms/3/1/2.png'
        '/asdf/asdf'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        '/asdf/asdf'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf/asdf'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf/asdf'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        '//asdf//asdf/'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        '//asdf//asdf/'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf//asdf/'     | 'qwer' |  true     | true     | '/asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
        'asdf//asdf/'     | 'qwer' |  true     | false     | 'asdf/asdf/r/e/w/q/qwer/tms/3/1/2.png'
    }
}
