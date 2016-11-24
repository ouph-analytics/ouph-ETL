var path = require('path');
var env = process.env.NODE_ENV || 'development';
env = env.toLowerCase();
var file = path.resolve(__dirname, env);
try {
    var config = module.exports = require(file);
    console.log('Load config: [%s] %s', env, file);
} catch (err) {
    console.error('Cannot load config: [%s] %s', env, file);
    throw err;
}
