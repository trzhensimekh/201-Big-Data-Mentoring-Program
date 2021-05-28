import static java.util.Calendar.*

// Turns all month, year, day fields in the following format dd/MM/yyyy

// returns month number from following inputs: 1,  "5", "Jan"
static def get_month(month) {
    if (month instanceof String && month.isInteger()) {
        return month as int
    }
    if (month instanceof String) {
        def d = Date.parse('MMM', month)
        return d.format('MM') as int
    }
    return month as int
}

static def get_date(map) {
    if (map['Day'] == null) {
        map['Day'] = 1
    }

    String pattern = 'dd/MM/yyyy'
    def date = new Date()

    date.set(
            year: map['Year'] as int,
            month: get_month(map['Month']) - 1,
            dayOfMonth: map['Day'] as int)

    date = date.format(pattern)
}

records = sdc.records
for (record in records) {
    try {
        map_date = record.value['article']['MedlineCitation']['DateCompleted']
        record.value['article']['MedlineCitation']['DateCompleted'] = get_date(map_date)

        map_date = record.value['article']['MedlineCitation']['DateRevised']
        record.value['article']['MedlineCitation']['DateRevised'] = get_date(map_date)

        map_date = record.value['article']['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate']
        record.value['article']['MedlineCitation']['Article']['Journal']['JournalIssue']['PubDate'] = get_date(map_date)

        list_map_date = record.value['article']['PubmedData']['History']['PubMedPubDate']

        list_map_date.each { map_date ->
            date = get_date(map_date)
            map_date['date'] = date

            map_date.remove('Year')
            map_date.remove('Month')
            map_date.remove('Day')
        }

        record.value['article']['PubmedData']['History']['PubMedPubDate'] = list_map_date

        sdc.output.write(record)
    } catch (e) {
        // Write a record to the error pipeline
        sdc.log.error(e.toString(), e)
        sdc.error.write(record, e.toString())
    }
}