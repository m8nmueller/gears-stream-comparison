import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

const baseUrl = 'http://localhost:8044';
const urlA = baseUrl + '/sensorA';
const urlB = baseUrl + '/sensorB';

const rparams = {
  probA: 0.4,
  sizeA: 12,
  probAsucc: 0.8,
  avgA: 49.0,
  stdA: 5.0,
  devMinA: 2.0,
  devMaxA: 3.3,
  minB: 4,
  maxB: 9,
  avgB: 46.0,
  stdB: 5.5
};

export const options = {
  // A number specifying the number of VUs to run concurrently.
  //vus: 10,
  // A string specifying the total duration of the test run.
  //duration: '30s',
  stages: [
    { duration: "10s", target: 2500 },
    { duration: "5s", target: 2500 },
    { duration: "10s", target: 5000 },
    { duration: "5s", target: 5000 },
    { duration: "10s", target: 7500 },
    { duration: "5s", target: 7500 },
    { duration: "10s", target: 10000 },
    { duration: "5s", target: 10000 },
    { duration: "10s", target: 12500 },
    { duration: "5s", target: 12500 },
    { duration: "10s", target: 15000 },
    { duration: "5s", target: 15000 },
    { duration: "10s", target: 17500 },
    { duration: "5s", target: 17500 },
    { duration: "10s", target: 20000 },
    { duration: "5s", target: 20000 },
    { duration: "10s", target: 22500 },
    { duration: "5s", target: 22500 },
    { duration: "10s", target: 25000 },
    { duration: "5s", target: 25000 },
    { duration: "10s", target: 0 },
  ],
};

// https://mika-s.github.io/javascript/random/normal-distributed/2019/05/15/generating-normally-distributed-random-numbers-in-javascript.html
const boxMullerTransform = () => {
  const u1 = Math.random();
  const u2 = Math.random();

  const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
  const z1 = Math.sqrt(-2.0 * Math.log(u1)) * Math.sin(2.0 * Math.PI * u2);

  return { z0, z1 };
};

const normalRnd = (mean, stddev) => {
  const { z0, _ } = boxMullerTransform();
  return z0 * stddev + mean;
};

const randomFloat = (min, max) => Math.random() * (max - min) + min;
const randomInt = (min, max) => Math.floor(randomFloat(min, max));

const genRequestA = () => {
  const reqs = [];
  for (let i = 0; i < rparams.sizeA; i++) {

    if (Math.random() < rparams.probAsucc) {
      const reading = normalRnd(rparams.avgA, rparams.stdA);
      const dev = randomFloat(rparams.devMinA, rparams.devMaxA);
      reqs[i] = { "reading": reading, "deviation": dev };
    } else {
      reqs[i] = null;
    }
  }

  const time = exec.scenario.iterationInTest * 200;
  return { "timestamp": time, "measurements": reqs };
};

const genRequestB = () => {
  const count = randomInt(rparams.minB, rparams.maxB);
  const arr = [];
  for (let i = 0; i < count; i++) {
    arr[i] = normalRnd(rparams.avgB, rparams.stdB);
  }

  const time = exec.scenario.iterationInTest * 200;
  return { "timestamp": time, "measurements": arr };
};

const isA = Math.random() < rparams.probA;
const url = isA ? urlA : urlB;
const gen = isA ? genRequestA : genRequestB;

export default function () {
  const payload = JSON.stringify(gen());

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  http.post(url, payload, params);
  sleep(0.2);
}

export function handleSummary(data) {
  return {
    'summary.json': JSON.stringify(data), //the default data object
    'stdout': textSummary(data),
  };
}

