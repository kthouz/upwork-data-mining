import sqlite3, os, glob
import myUpwork as uw
import numpy as np
from pprint import pprint

def closedb(cur,conn):
    """
    close all connection to the database
    """
    conn.commit()
    cur.close()
    conn.close()

def designdb(cur):
    """
    build the database
    """
    # create users table
    cur.execute('DROP TABLE IF EXISTS users')
    cur.execute('CREATE TABLE users('
                'id TEXT PRIMARY KEY UNIQUE NOT NULL, '
                'name TEXT, '
                'country TEXT, '
                'city TEXT, '
                'profileType TEXT, '
                'title TEXT, '
                'description TEXT, '
                'feedbackScore REAL, '
                'rate REAL, '
                'testCount INTEGER, '
                'portfolioCount INTEGER, '
                'scoreRecent REAL, '
                'score REAL, '
                'billedAssignmentsCount INTEGER, '
                'totalHours REAL, '
                'englishSkill REAL, '
                'isAffiliated INTEGER, '
                'memberSince TEXT, '
                'lastActivity TEXT, '
                'lastWorked TEXT, '
                'timezone TEXT)'
                )
    
    # create user assignments table
    cur.execute('DROP TABLE IF EXISTS uAssignments')
    cur.execute('CREATE TABLE uAssignments('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'uwAssignmentId TEXT NOT NULL, '
                'agencyName TEXT, '
                'openingTitle TEXT, '
                'rate REAL, '
                'totalCharge REAL, '
                'totalHours REAL, '
                'feedbackScore REAL, '
                'feedbackComment TEXT, '
                'asFrom TEXT, '
                'asTo TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )

    #create feedbacks table
    cur.execute('DROP TABLE IF EXISTS uFeedbacks')
    cur.execute('CREATE TABLE uFeedbacks('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'uwAssignmentId TEXT NOT NULL, '
                'skillsScore REAL, '
                'qualityScore REAL, '
                'availabilityScore REAL, '
                'deadlineScore REAL, '
                'communicationScore REAL, '
                'cooperationScore REAL, '
                'FOREIGN KEY(uwAssignmentId) REFERENCES uAssignments(uwAssignmentId), '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
                
    # create user portrait table
    cur.execute('DROP TABLE IF EXISTS uPortraits')
    cur.execute('CREATE TABLE uPortraits('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'link TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
    
    # create user education table
    cur.execute('DROP TABLE IF EXISTS uEducation')
    cur.execute('CREATE TABLE uEducation('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'area TEXT, '
                'degree TEXT, '
                'school TEXT, '
                'asFrom TEXT, '
                'asTo TEXT, '
                'comment TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
                
    # create user exams table
    cur.execute('DROP TABLE IF EXISTS uExams')
    cur.execute('CREATE TABLE uExams('
                'id TEXT PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'examId TEXT NOT NULL, '
                'name TEXT NOT NULL, '
                'pass INTEGER, '
                'score REAL, '
                'percentile REAL, '
                'duration REAL, '
                'asWhen TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
    
    # create user job categories table
    cur.execute('DROP TABLE IF EXISTS uCategories')
    cur.execute('CREATE TABLE uCategories('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'firstLevel TEXT, '
                'secondLevel TEXT, '
                'seoLink TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
    
    # create user skills table
    cur.execute('DROP TABLE IF EXISTS uSkills')
    cur.execute('CREATE TABLE uSkills('
                'id INTEGER PRIMARY KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'name TEXT NOT NULL, '
                'rank INTEGER, '
                'hasTests INTEGER, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
    
    # create user experiences
    cur.execute('DROP TABLE IF EXISTS uExperiences')
    cur.execute('CREATE TABLE uExperiences('
                'id TEXT PRIMARy KEY UNIQUE, '
                'userId TEXT NOT NULL, '
                'company TEXT, '
                'title TEXT, '
                'asFrom TEXT, '
                'asTo TEXT, '
                'comment TEXT, '
                'FOREIGN KEY(userId) REFERENCES users(id))'
                )
    print "database created"

def bypassNone(x,key):
    return None if x == None else x.get(key)

def nonetozero(x,key):
    return 0 if x.get(key) == None else x.get(key)

def insertData(cur, conn, d1, d2):
    """
    This function works exclusively with json downloaded from upwork.
    To insert data from other source, please consider reviewing documentation
    or writing a custom function
    """
    # insert data in users table
    uInfo = (d1.get('id'), d1.get('name'), d1.get('country'), d2.get('dev_city'),
             d1.get('profile_type'),d1.get('title'), d1.get('description'),
             d1.get('feedback'), d1.get('rate'), d1.get('test_passed_count'),
             d1.get('portfolio_items_count'), d2.get('dev_adj_score_recent'),
             d2.get('dev_adj_score'), d2.get('dev_billed_assignments'),
             d2.get('dev_total_hours'), d2.get('dev_eng_skill'), d2.get('dev_is_affiliated'),
             d1.get('member_since'), d1.get('last_activity'), d2.get('dev_last_worked_ts'),
             d2.get('dev_timezone'))
    cur.execute('INSERT OR IGNORE INTO users('
                'id, '
                'name, '
                'country, '
                'city, '
                'profileType, '
                'title, '
                'description, '
                'feedbackScore, '
                'rate, '
                'testCount, '
                'portfolioCount, '
                'scoreRecent, '
                'score, '
                'billedAssignmentsCount, '
                'totalHours, '
                'englishSkill, '
                'isAffiliated, '
                'memberSince, '
                'lastActivity, '
                'lastWorked, '
                'timezone) VALUES '
                '(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                uInfo)

    # insert data into portrait
    cur.execute('INSERT OR IGNORE INTO uPortraits('
                'id, '
                'userId, '
                'link) VALUES '
                '(NULL,?,?)',
                (d1.get('id'), d2.get('dev_portrait_50')))
    
    # insert data into uAssignments

    for k in d2['assignments'].keys():
        if d2['assignments'][k] != '':
            a = d2['assignments'][k]['job']
            if type(a) == type([]):
                for l in a:
                    aData = (d1.get('id'),
                             l.get('as_ciphertext_opening_recno'),
                             l.get('as_agency_name'),
                             l.get('as_opening_title'),
                             nonetozero(l,'as_rate'),
                             #l.get('as_rate'),
                             nonetozero(l,'as_total_charge'),
                             #l.get('as_total_charge'),
                             l.get('as_total_hours'),
                             bypassNone(l.get('feedback'),'score'),
                             bypassNone(l.get('feedback'),'comment'),
                             l.get('as_from_full'),
                             l.get('as_to'))

                    cur.execute('INSERT OR IGNORE INTO uAssignments('
                                'id, userId, uwAssignmentId, agencyName, openingTitle, '
                                'rate, totalCharge, totalHours, feedbackScore, '
                                'feedbackComment, asFrom, asTo) VALUES '
                                '(NULL,?,?,?,?,?,?,?,?,?,?,?)', aData)
            if type(a) == type({}):
                aData = (d1.get('id'),
                         a.get('as_ciphertext_opening_recno'),
                         a.get('as_agency_name'),
                         a.get('as_opening_title'),
                         nonetozero(a,'as_rate'),
                         #l.get('as_rate'),
                         nonetozero(a,'as_total_charge'),
                         #a.get('as_total_charge'),
                         a.get('as_total_hours'),
                         bypassNone(a.get('feedback'),'score'),
                         bypassNone(a.get('feedback'),'comment'),
                         a.get('as_from_full'),
                         a.get('as_to'))
                cur.execute('INSERT OR IGNORE INTO uAssignments('
                            'id, userId, uwAssignmentId, agencyName, openingTitle, '
                            'rate, totalCharge, totalHours, feedbackScore, '
                            'feedbackComment, asFrom, asTo) VALUES '
                            '(NULL,?,?,?,?,?,?,?,?,?,?,?)', aData)
            
    # inseert data into uFeedback
    for k in d2['assignments'].keys():
        if d2['assignments'][k] != '':
            a = d2['assignments'][k]['job']
            if type(a) == type([]):
                for l in a:
                    f = {}
                    if l.get('feedback') != None:
                        for sc in l['feedback']['scores']['score']:
                            f[sc['label']] = sc['score']
                        fData = (d1.get('id'),
                                 l.get('as_ciphertext_opening_recno'),
                                 f.get('Skills'),
                                 f.get('Quality'),
                                 f.get('Availability'),
                                 f.get('Deadlines'),
                                 f.get('Communication'),
                                 f.get('Cooperation'))
                        cur.execute('INSERT OR IGNORE INTO uFeedbacks('
                                    'id, userId, uwAssignmentId, skillsScore, '
                                    'qualityScore , availabilityScore, deadlineScore, '
                                    'communicationScore, cooperationScore) VALUES '
                                    '(NULL,?,?,?,?,?,?,?,?)', fData)
                             
            if type(a) == type({}):
                f = {}
                if a.get('feedback') != None:
                    for sc in a['feedback']['scores']['score']:
                        f[sc['label']] = sc['score']
                    fData = (d1.get('id'),
                             a.get('as_ciphertext_opening_recno'),
                             f.get('Skills'),
                             f.get('Quality'),
                             f.get('Availability'),
                             f.get('Deadlines'),
                             f.get('Communication'),
                             f.get('Cooperation'))
                    cur.execute('INSERT OR IGNORE INTO uFeedbacks('
                                'id, userId, uwAssignmentId, skillsScore, '
                                'qualityScore , availabilityScore, deadlineScore, '
                                'communicationScore, cooperationScore) VALUES '
                                '(NULL,?,?,?,?,?,?,?,?)', fData)

    #insert into education table
    if d2.get('education') != '':
        if type(d2['education'].get('institution')) == type ([]):
            for institution in d2['education'].get('institution'):
                cur.execute('INSERT OR IGNORE INTO uEducation('
                            'id, userId, area, degree, school, '
                            'asFrom, asTo, comment) VALUES '
                            '(NULL,?,?,?,?,?,?,?)',
                            (d1.get('id'),
                             institution.get('ed_area'),
                             institution.get('ed_degree'),
                             institution.get('ed_school'),
                             institution.get('ed_from'),
                             institution.get('ed_to'),
                             institution.get('ed_comment')))
        if type(d2['education'].get('institution')) == type({}):
            institution = d2['education']['institution']
            cur.execute('INSERT OR IGNORE INTO uEducation('
                        'id, userId, area, degree, school, '
                        'asFrom, asTo, comment) VALUES '
                        '(NULL,?,?,?,?,?,?,?)',
                        (d1.get('id'),
                         institution.get('ed_area'),
                         institution.get('ed_degree'),
                         institution.get('ed_school'),
                         institution.get('ed_from'),
                         institution.get('ed_to'),
                         institution.get('ed_comment')))
            
    #insert into exams table
    if d2.get('tsexams') != '':
        if type(d2['tsexams']['tsexam']) == type([]):
            for exam in d2['tsexams']['tsexam']:
                cur.execute('INSERT OR IGNORE INTO uExams('
                            'id, userId, examId, name, pass, score, '
                            'percentile, duration, asWhen) VALUES '
                            '(NULL,?,?,?,?,?,?,?,?)',
                            (d1.get('id'),
                             exam.get('ts_id'),
                             exam.get('ts_name_raw'),
                             exam.get('ts_pass'),
                             exam.get('ts_score'),
                             exam.get('ts_percentile'),
                             exam.get('ts_duration'),
                             exam.get('ts_when')))
        if type(d2['tsexams']['tsexam']) == type ({}):
            exam = d2['tsexams']['tsexam']
            cur.execute('INSERT OR IGNORE INTO uExams('
                        'id, userId, examId, name, pass, score, '
                        'percentile, duration, asWhen) VALUES '
                        '(NULL,?,?,?,?,?,?,?,?)',
                        (d1.get('id'),
                         exam.get('ts_id'),
                         exam.get('ts_name_raw'),
                         exam.get('ts_pass'),
                         exam.get('ts_score'),
                         exam.get('ts_percentile'),
                         exam.get('ts_duration'),
                         exam.get('ts_when')))

    #insert categories to table
    if d2.get('job_categories') != None:
        if type(d2['job_categories']['job_category']) == type([]):
            for category in d2['job_categories']['job_category']:
                cur.execute('INSERT OR IGNORE INTO uCategories('
                            'id, userId, firstLevel, secondLevel, '
                            'seoLink) VALUES '
                            '(NULL,?,?,?,?)',
                            (d1.get('id'),
                             category.get('first_level'),
                             category.get('second_level'),
                             category.get('seo_link')))
        if type(d2['job_categories']['job_category']) == type({}):
            category = d2['job_categories']['job_category']
            cur.execute('INSERT OR IGNORE INTO uCategories('
                        'id, userId, firstLevel, secondLevel, '
                        'seoLink) VALUES '
                        '(NULL,?,?,?,?)',
                        (d1.get('id'),
                         category.get('first_level'),
                         category.get('second_level'),
                         category.get('seo_link')))

    #insert skills to table
    if d2.get('skills')!= '':
        if type(d2['skills']['skill']) == type([]):
            for skill in d2['skills']['skill']:
                cur.execute('INSERT OR IGNORE INTO uSkills('
                            'id, userId, name, rank, hasTests) VALUES '
                            '(NULL,?,?,?,?)',
                            (d1.get('id'),
                             skill.get('skl_name'),
                             skill.get('skl_rank'),
                             skill.get('skl_has_tests')))
        if type(d2['skills']['skill']) == type({}):
            skill = d2['skills']['skill'];
            cur.execute('INSERT OR IGNORE INTO uSkills('
                        'id, userId, name, rank, hasTests) VALUES '
                        '(NULL,?,?,?,?)',
                        (d1.get('id'),
                         skill.get('skl_name'),
                         skill.get('skl_rank'),
                         skill.get('skl_has_tests')))

    #insert experiences to table
    if d2.get('experiences') != '':
        if type(d2['experiences']['experience']) == type([]):
            for experience in d2['experiences']['experience']:
                cur.execute('INSERT OR IGNORE INTO uExperiences('
                            'id, userId, company, title, '
                            'asFrom, asTo, comment) VALUES '
                            '(NULL,?,?,?,?,?,?)',
                            (d1.get('id'),
                             experience.get('exp_company'),
                             experience.get('exp_title'),
                             experience.get('exp_from'),
                             experience.get('exp_to'),
                             experience.get('exp_comment')))
        if type(d2['experiences']['experience']) == type({}):
            experience = d2['experiences']['experience']
            cur.execute('INSERT OR IGNORE INTO uExperiences('
                        'id, userId, company, title, '
                        'asFrom, asTo, comment) VALUES '
                        '(NULL,?,?,?,?,?,?)',
                        (d1.get('id'),
                         experience.get('exp_company'),
                         experience.get('exp_title'),
                         experience.get('exp_from'),
                         experience.get('exp_to'),
                         experience.get('exp_comment')))
                
            
    conn.commit()
    pass

if __name__ == '__main__':
    
    directory = '/Users/cgirabawe/SideProjects/test'
    directory = '/Users/cgirabawe/SideProjects/upwork_data_mining/upwork_project'
    directory = '/Users/cgirabawe/SideProjects/upwork_data_mining'
    os.chdir(directory)
    conn = sqlite3.connect('../upworkdb.db')
    cur = conn.cursor()

    profiles = glob.glob(directory+'/data/profiles/*.json')
    details = glob.glob(directory+'/data/details/*.json')
    redesign = True
    if redesign: #True/False to redesign database if necessary
        designdb(cur)

    k = 0
    for detail in details:
        f = os.path.basename(detail)
        d1 = uw.loadJson(directory+'/data/profiles/'+f)
        d2 = uw.loadJson(directory+'/data/details/'+f)
        #try:
        insertData(cur,conn,d1,d2)
        #except:
#            uw.saveJson([],directory+'/skipped/'+f)
        k+=1
        if k%10000 == 0: print k
    closedb(cur,conn)
