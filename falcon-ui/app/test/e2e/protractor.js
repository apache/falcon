exports.config = {
  seleniumAddress: 'http://localhost:4444/wd/hub',
  specs: ['*E2E.js'],
  capabilities: {
    'browserName': 'firefox'
  }
};