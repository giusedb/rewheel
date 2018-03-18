from .baseapp import app
from rewheel import DAL, Field
from datetime import datetime

@app.create_db
def db_define(app):
    return DAL(app,'sqlite://db_testset.db',folder='testingapp/dbtestset')


@app.define_db
def define(app,db):
    db.define_table('master_object',
                    Field('name',length=100),
                    Field('length','integer'),
                    Field('width','float'),
                    Field('height','float'),
                    Field('created_on','datetime',default=lambda x : datetime.now()),
                    Field('created_by',db.auth_user, default=lambda x : app.auth.user_id),
    )

    db.define_table('related_object',
                    Field('name',length=100),
                    Field('related_to',db.master_object),
                    Field('updated_on', 'datetime', compute = lambda x : datetime.now()),
                    Field('updated_by',db.auth_user, compute = lambda x : app.auth.user_id),
                    )


    db.define_table('related_c_object',
                    Field('name',length=100),
                    Field('related_to',db.master_object),
                    Field('updated_on', 'datetime', compute = lambda x : datetime.now()),
                    Field('updated_by',db.auth_user, compute = lambda x : app.auth.user_id),
                    Field('created_on','datetime',default=lambda x : datetime.now()),
                    Field('created_by',db.auth_user, default=lambda x : app.auth.user_id),
                    )