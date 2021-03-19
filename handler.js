'use strict';

const ftp = require("basic-ftp");
const xml2js = require('xml2js');
const fs = require('fs');
const awsParamStore = require('aws-param-store');
const { DateTime } = require('luxon');
const Holidays = require('date-holidays');
const hd = new Holidays();
const installations = process.env.INSTALLATIONS.split(",");

module.exports.tomorrow = async event => {
    try {
        const tomorrow = new Date();
        tomorrow.setDate(new Date().getDate() + 1);
        let tomorrowString = tomorrow.toISOString().substring(0, 10).replace(/-/g, "");
        return await import_gme_data_by_date(tomorrowString, event);
    } catch (error) {
        logger('ERROR', error);
        return response(400, error, event);
    }
};

module.exports.by_date = async event => {
    try {
        let date = event.queryStringParameters.date.replace(/-/g, "");
        return await import_gme_data_by_date(date, event);
    } catch (error) {
        logger('ERROR', error);
        return response(400, error, event);
    }
};

async function import_gme_data_by_date(date, event) {
    hd.init('IT');
    logger('INFO', `Start import PUN for ${date}`);
    await download_file(date);
    let json = await xml2json();
    let messages = parseData(json);
    logger('INFO', JSON.stringify(messages));
    await sendMessages(messages);
    logger('INFO', `End import PUN for ${date}`);
    return response(200, messages, event);
}

const response = (code, message, event) => {
    return {
        statusCode: code,
        body: JSON.stringify(
            {
                message: message,
                input: event
            },
            null,
            2
        ),
    };
}

async function download_file(date) {
    const client = new ftp.Client();
    client.ftp.verbose = true;
    const GME_PASSWORD = awsParamStore.getParameterSync(process.env.GME_PASSWORD);
    try {
        await client.access({
            host: "download.mercatoelettrico.org",
            user: process.env.GME_USER,
            password: GME_PASSWORD.Value,
            secure: false
        })
        logger('INFO', await client.list());
        await client.downloadTo("/tmp/temp.xml", `/MercatiElettrici/MGP_Prezzi/${date}MGPPrezzi.xml`);
    }
    catch(err) {
        logger('ERROR', err);
    }
    client.close();
}

function xml2json() {
    const parser = new xml2js.Parser();
    return new Promise((resolve, reject) => {
        fs.readFile('/tmp/temp.xml', function(err, data) {
            if(err) {
                reject(err);
            }
            parser.parseString(data, function (err, result) {
                if (err) {
                    reject(err);
                }
                resolve(result);
            });
        });
    });
}

async function sendMessages(messages) {
    const AMQP_URL = awsParamStore.getParameterSync(process.env.AMQP_URL);
    const QUEUE_NAME = process.env.QUEUE_NAME;
    const { Connection } = require('amqplib-as-promised');
    const connection = new Connection(AMQP_URL.Value);//parameter: full queue url
    await connection.init();
    const channel = await connection.createChannel();
    await channel.assertQueue(QUEUE_NAME);
    await channel.publish("amq.direct", QUEUE_NAME, Buffer.from(JSON.stringify(messages)));
    await channel.close();
    await connection.close();
}

function parseData(data) {
    let messages = [];
    let values = [];
    let peakload = [];
    let out_peakload = [];
    const date = define_date(data.NewDataSet.Prezzi[0].Data[0], parseInt(data.NewDataSet.Prezzi[0].Ora[0]));
    data.NewDataSet.Prezzi.forEach( value => {
        let date = define_date(value.Data[0], parseInt(value.Ora[0]));
        let pun = parseFloat(value.PUN[0].replace(/,/g,'.'));
        let hour = parseInt(value.Ora[0]) - 1;
        installations.forEach(installation => messages.push(simon_data("pun", pun, date.toUTC().toISO(), installation)));
        values.push(pun);
        if (is_out_peakload(hour, date)) {
            out_peakload.push(pun);
        } else {
            peakload.push(pun);
        }
    });
    installations.forEach(installation => {
        push(messages, simon_data("max_pun", max(values), date.toUTC().toISO(), installation));
        push(messages, simon_data("min_pun", min(values), date.toUTC().toISO(), installation));
        push(messages, simon_data("avg_pun", average(values), date.toUTC().toISO(), installation));
        push(messages, simon_data("out_peakload_pun", average(out_peakload), date.toUTC().toISO(), installation));
        push(messages, simon_data("peakload_pun", average(peakload), date.toUTC().toISO(), installation));
    });
    return messages;
}

function define_date(date_string, hour) {
    return DateTime.fromObject({
        year: parseInt(date_string.substring(0, 4)),
        month: parseInt(date_string.substring(4, 6)),
        day: parseInt(date_string.substring(6, 8)),
        hour: hour - 1,
        minute: 0,
        second: 0,
        millisecond: 0,
        zone: 'Europe/Rome'
    });
}

function simon_data(datapoint, value, timestamp, installation) {
    return {
        device: "gme",
        measurementUnit: "€/MWh",
        installation: installation,
        name: datapoint,
        value: value,
        status: 0,
        timestamp: timestamp
    }
}

function average(nums) {
    return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function max(values) {
    return Math.max(...values);
}

function min(values) {
    return Math.min(...values);
}

function push(values, data) {
    if (data.value !== null && data.value !== undefined && !isNaN(data.value)) {
        values.push(data);
    }
}

//Le ore di Fuori Picco sono, nei giorni dal lunedì al venerdì, le ore comprese tra le 00.00 e le 08.00 e tra le 20.00 e le 24.00 e, nei giorni di sabato e domenica e festivi, tutte le ore.
function is_out_peakload(hour, date) {
    return hour < 8 || hour >= 20 || date.toJSDate().getDay() === 6 || date.toJSDate().getDay() === 0  || hd.isHoliday(date.toJSDate())
}

const logger = (severity = 'INFO', msg) => {
    console.log(`${DateTime.local().toUTC().toISO()} - ${severity}: ${msg}`)
}
