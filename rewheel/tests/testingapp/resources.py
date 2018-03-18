from baseapp import app
from rewheel import TableResource, ManyToManyRelation

@app.register_resources
def all_tables(app,db):
    for table in db.tables:
        yield TableResource(db[table])