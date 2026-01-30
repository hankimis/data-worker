/**
 * Windows Service Installation Script
 *
 * Usage:
 *   npm run build
 *   node scripts/install-service.js
 *
 * To uninstall:
 *   node scripts/install-service.js uninstall
 */

const Service = require('node-windows').Service;
const path = require('path');

const svc = new Service({
  name: 'IOV Data Collector',
  description: 'IOV Viral SaaS Data Collection Worker',
  script: path.join(__dirname, '..', 'dist', 'index.js'),
  nodeOptions: [],
  workingDirectory: path.join(__dirname, '..'),
  allowServiceLogon: true,
});

svc.on('install', () => {
  console.log('Service installed successfully!');
  console.log('Starting service...');
  svc.start();
});

svc.on('start', () => {
  console.log('Service started!');
  console.log('The service is now running in the background.');
});

svc.on('uninstall', () => {
  console.log('Service uninstalled successfully!');
});

svc.on('error', (err) => {
  console.error('Service error:', err);
});

// Check command
const command = process.argv[2];

if (command === 'uninstall') {
  console.log('Uninstalling IOV Data Collector service...');
  svc.uninstall();
} else {
  console.log('Installing IOV Data Collector as Windows service...');
  console.log('Script path:', svc.script);
  console.log('Working directory:', path.join(__dirname, '..'));
  svc.install();
}
