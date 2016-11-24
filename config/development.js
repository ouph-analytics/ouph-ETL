module.exports = {
  etl: {
    queue: 'mysql',
    db: 'mongodb'
  },
  conn: {
    queue: {
      connectionLimit: 100,
      host: 'localhost',
      user: 'root',
      password: 'root',
      database: 'tracking',
      dateStrings: false,
      multipleStatements: true
    },
    db: {
      connectionLimit: 100,
      host: 'localhost',
      user: 'root',
      password: 'root',
      database: 'tracking',
      dateStrings: false,
      multipleStatements: true
    }
  },
  domain: ['localhost']


};