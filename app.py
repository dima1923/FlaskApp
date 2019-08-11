from flask import Flask
from flask.json import jsonify
import sqlite3
from flask import request

app = Flask(__name__)


def conn(sqlCode, par=None):
    con = sqlite3.connect("/home/dmitry/Desktop/my.db")
    con.row_factory = sqlite3.Row
    c = con.cursor()
    if(par==None):
        c.execute(sqlCode)
    else:
        c.execute(sqlCode, par)
    ans = [dict(i) for i in c.fetchall()]
    con.close()
    return jsonify(ans)


@app.route('/todo/api/v1.0/Survey-Data', methods=['GET'])
def get_table():
    return conn("SELECT * FROM `Survey-Data`")


@app.route('/todo/api/v1.0/Survey-Data/EmploymentField/<string:ef>', methods=['GET'])
def get_EmpFie(ef):
    return conn("SELECT * from `Survey-Data` WHERE EmploymentField=:emlfiel", {'emlfiel': ef})


@app.route('/todo/api/v1.0/Survey-Data/EmploymentStatus/<string:es>', methods=['GET'])
def get_EmpSt(es):
    return conn("SELECT * from `Survey-Data` WHERE EmploymentStatus=:es", {'es': es})


@app.route('/todo/api/v1.0/Survey-Data/Gender/<string:gen>', methods=['GET'])
def get_Gen(gen):
    return conn("SELECT * from `Survey-Data` WHERE Gender=:gen", {'gen': gen})


@app.route('/todo/api/v1.0/Survey-Data/JobPref/<string:jp>', methods=['GET'])
def get_JobPref(jp):
    return conn("SELECT * FROM `Survey-Data` WHERE JobPref=:jp", {'jp': jp})


@app.route('/todo/api/v1.0/Survey-Data/JobWherePref/<string:jwp>', methods=['GET'])
def get_JobWherePref(jwp):
    return conn("SELECT * FROM `Survey-Data` WHERE JobWherePref=:jwp", {'jwp': jwp})


@app.route('/todo/api/v1.0/Survey-Data/MaritalStatus/<string:ms>', methods=['GET'])
def get_MarSta(ms):
    return conn("SELECT * FROM `Survey-Data` WHERE MaritalStatus=:ms", {'ms': ms})


@app.route('/todo/api/v1.0/Survey-Data/Income/<float:inc>', methods=['GET'])
def get_Income(inc):
    return conn("SELECT * FROM `Survey-Data` WHERE Income=:inc", {'inc': inc})


@app.route('/todo/api/v1.0/Survey-Data/ID/<string:idi>',methods=['GET'])
def get_ID(idi):
    return conn("SELECT * FROM `Survey-Data` WHERE ID=:idi",{'idi':idi})


@app.route('/todo/api/v1.0/Survey-Data',methods=['POST'])
def create_SurDa():
    con = sqlite3.connect("/home/dmitry/Desktop/my.db")
    l=con.cursor()
    ind=int(l.execute("SELECT MAX(ID) from `Survey-Data`").fetchone()[0])
    ind=str(ind+1)
    """obj={
        'ID':ind,
        'EmploymentField':request.json['EmploymentField'],
        'EmploymentStatus':request.json['EmploymentStatus'],
        'Gender':request.json['Gender'],
        'JobPref':request.json['Gender'],
        'JobWherePref':request.json['JobWherePref'],
        'MaritalStatus':request.json['MaritalStatus'],
        'Income':request.json['Income']
    }"""
    #print(request.json)
    obj=request.json
    obj['ID']=ind
    con.row_factory = sqlite3.Row
    c = con.cursor()
    if(request.json!=None):
        c.execute("INSERT INTO `Survey-Data`(ID,EmploymentField,EmploymentStatus,Gender,JobPref,JobWherePref,MaritalStatus,Income) VALUES (:ID,:EmploymentField,:EmploymentStatus,:Gender,:JobPref,:JobWherePref,:MaritalStatus,:Income)",
                  obj)
    con.commit()
    con.close()
    return jsonify(obj),201

@app.route('/todo/api/v1.0/Survey-Data/<string:idi>',methods=['DELETE'])
def row_del(idi):
    con = sqlite3.connect("/home/dmitry/Desktop/my.db")
    l = con.cursor()
    l.execute("DELETE FROM `Survey-Data` WHERE ID=:ID",{'ID':idi})
    con.commit()
    con.close()
    return jsonify({'result':'true'})

@app.route('/todo/api/v1.0/Survey-Data/byGender', methods=['GET'])
def get_byGender():
    return conn("SELECT Gender AS Gender, COUNT(*) AS COUNTER from `Survey-Data` GROUP BY Gender")

@app.route('/todo/api/v1.0/Survey-Data/<string:idi>',methods=['PUT'])
def update_row(idi):
    con = sqlite3.connect("/home/dmitry/Desktop/my.db")
    con.row_factory = sqlite3.Row
    l = con.cursor()
    l.execute("SELECT * FROM `Survey-Data` WHERE ID=:ID",{'ID':idi})
    tmp=dict(l.fetchone())
    if 'EmploymentField' in request.json:
        tmp['EmploymentField']=request.json['EmploymentField']
    if 'EmploymentStatus' in request.json:
        tmp['EmploymentStatus']=request.json['EmploymentStatus']
    if 'Gender' in request.json:
        tmp['Gender']=request.json['Gender']
    if 'JobPref' in request.json:
        tmp['JobPref']=request.json['JobPref']
    if 'JobWherePref' in request.json:
        tmp['JobWherePref']=request.json['JobWherePref']
    if 'MaritalStatus' in request.json:
        tmp['MaritalStatus']=request.json['MaritalStatus']
    if 'Income' in request.json:
        tmp['Income']=request.json['Income']
    l.execute("UPDATE `Survey-Data` SET EmploymentField=:EmploymentField,EmploymentStatus=:EmploymentStatus,Gender=:Gender,JobPref=:JobPref,JobWherePref=:JobWherePref,MaritalStatus=:MaritalStatus,Income=:Income WHERE ID=:ID", tmp)
    con.commit()
    con.close()
    return jsonify({"result":'true'})

if __name__ == '__main__':
    app.run()
