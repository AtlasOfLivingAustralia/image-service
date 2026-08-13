package au.org.ala.images

import grails.converters.JSON
import grails.gorm.transactions.Transactional
import groovy.json.JsonSlurper

class UserPreferencesService {

    UserPreferences getUserPreferences(String userId) {
        if (userId) {
            def prefs = UserPreferences.findByUserId(userId)
            if (!prefs) {
                prefs = new UserPreferences(userId: userId)
                prefs.save()
            }
            return prefs
        }
        return null
    }

    List<CSVColumnDefintion> getUserColumnDefinitions(String userId) {
        List<CSVColumnDefintion> coldefs = []
        def prefs  = getUserPreferences(userId)
        if (prefs) {
            if (prefs.exportColumns) {
                def list = new JsonSlurper().parseText(prefs.exportColumns)
                list.each { Map colDef ->
                    coldefs << new CSVColumnDefintion(colDef)
                }
            }
        }
        return coldefs
    }

    void saveUserColumnDefintions(String userId, List<CSVColumnDefintion> columns) {
        def prefs  = getUserPreferences(userId)
        if (prefs) {
            prefs.exportColumns = new JSON(columns).toString()
            prefs.save()
        }
    }

}
