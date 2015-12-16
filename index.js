var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database(':memory:');

var ambiente1 = "DESA";
var ambiente2 = "UAT";

function cargarTabla (db, tabla, datos) {

  if (datos.length > 0) {

    var query = 'INSERT INTO ' + tabla;
    var fields = [];
    var values = [];
    for (var field in datos[0]) {
      fields.push(field);
      values.push('?');
    }
    query += ' (' + fields.join(',') +  ') VALUES (' + values.join(',') + ')';

    console.log(query);
    var stmt = db.prepare(query);

    for (var i = 0; i < datos.length; i++) {
      var data = [];
      for (var field in datos[i])
        data.push(datos[i][field]);
      stmt.run(data);
    }
    stmt.finalize();

  }

}

db.serialize(function () {

  db.run('CREATE TABLE DVM_SISTEMA_1 (ID NUMBER, NOMBRE TEXT, DESCRIPCION TEXT, PAIS TEXT)');
  db.run('CREATE TABLE DVM_ENTIDAD_CANONICA_1 (ID NUMBER, NOMBRE TEXT, DESCRIPCION TEXT)');
  db.run('CREATE TABLE DVM_VALOR_CANONICO_1 (ID NUMBER, ID_ENTIDAD_CANONICA NUMBER, DESCRIPCION TEXT, VALOR_CANONICO TEXT)');
  db.run('CREATE TABLE DVM_VALOR_SISTEMA_1 (ID NUMBER, ID_SISTEMA NUMBER, ID_ENTIDAD_CANONICA NUMBER, ID_VALOR_CANONICO NUMBER, VALOR_SISTEMA TEXT)');

  db.run('CREATE TABLE DVM_SISTEMA_2 (ID NUMBER, NOMBRE TEXT, DESCRIPCION TEXT, PAIS TEXT)');
  db.run('CREATE TABLE DVM_ENTIDAD_CANONICA_2 (ID NUMBER, NOMBRE TEXT, DESCRIPCION TEXT)');
  db.run('CREATE TABLE DVM_VALOR_CANONICO_2 (ID NUMBER, ID_ENTIDAD_CANONICA NUMBER, DESCRIPCION TEXT, VALOR_CANONICO TEXT)');
  db.run('CREATE TABLE DVM_VALOR_SISTEMA_2 (ID NUMBER, ID_SISTEMA NUMBER, ID_ENTIDAD_CANONICA NUMBER, ID_VALOR_CANONICO NUMBER, VALOR_SISTEMA TEXT)');

  cargarTabla (db, 'DVM_SISTEMA_1', require('./DVM_SISTEMA_' + ambiente1 + '.json'));
  cargarTabla (db, 'DVM_SISTEMA_2', require('./DVM_SISTEMA_' + ambiente2 + '.json'));
  cargarTabla (db, 'DVM_ENTIDAD_CANONICA_1', require('./DVM_ENTIDAD_CANONICA_' + ambiente1 + '.json'));
  cargarTabla (db, 'DVM_ENTIDAD_CANONICA_2', require('./DVM_ENTIDAD_CANONICA_' + ambiente2 + '.json'));
  cargarTabla (db, 'DVM_VALOR_CANONICO_1', require('./DVM_VALOR_CANONICO_' + ambiente1 + '.json'));
  cargarTabla (db, 'DVM_VALOR_CANONICO_2', require('./DVM_VALOR_CANONICO_' + ambiente2 + '.json'));
  cargarTabla (db, 'DVM_VALOR_SISTEMA_1', require('./DVM_VALOR_SISTEMA_' + ambiente1 + '.json'));
  cargarTabla (db, 'DVM_VALOR_SISTEMA_2', require('./DVM_VALOR_SISTEMA_' + ambiente2 + '.json'));

  db.all("SELECT * FROM DVM_ENTIDAD_CANONICA_1 WHERE NOMBRE NOT IN (SELECT NOMBRE FROM DVM_ENTIDAD_CANONICA_2)", function (err, row) {
    if (err)
      console.log(err);
    console.log(row);
  });

  db.all("SELECT DVM_SISTEMA_1.NOMBRE as SISTEMA, DVM_ENTIDAD_CANONICA_1.NOMBRE as ENTIDAD, " +
          "DVM_VALOR_CANONICO_1.VALOR_CANONICO as VALOR_CANONICO, DVM_VALOR_SISTEMA_1.VALOR_SISTEMA AS VALOR " +
          "FROM DVM_VALOR_SISTEMA_1 LEFT JOIN DVM_SISTEMA_1 ON DVM_VALOR_SISTEMA_1.ID_SISTEMA = DVM_SISTEMA_1.ID " + 
          "LEFT JOIN DVM_ENTIDAD_CANONICA_1 ON DVM_VALOR_SISTEMA_1.ID_ENTIDAD_CANONICA = DVM_ENTIDAD_CANONICA_1.ID " +
          "LEFT JOIN DVM_VALOR_CANONICO_1 ON DVM_VALOR_SISTEMA_1.ID_VALOR_CANONICO = DVM_VALOR_CANONICO_1.ID", function (err, rows) {
    if (err)
      console.log(err);
    console.log(rows);
  });

  /*db.all("SELECT DVM_ENTIDAD_CANONICA_1.NOMBRE, DVM_VALOR_SISTEMA_1.ID_ENTIDAD_CANONICA FROM DVM_VALOR_SISTEMA_1 LEFT JOIN DVM_ENTIDAD_CANONICA_1 ON DVM_VALOR_SISTEMA_1.ID_ENTIDAD_CANONICA = DVM_ENTIDAD_CANONICA_1.ID", function (err, rows) {
    if (err)
      console.log(err);
    console.log(rows);
  });*/

  db.close();

});

