const config = require("./config");
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
    host: 'ec2-52-10-42-119.us-west-2.compute.amazonaws.com:9200'
});


function getNumberOfP() {
    return new Promise(resolve => {
        client.search({
            index: config.elasticIndex + "-p0",
            body: {
                query: {
                    match_all: {}
                }
            }
        }).then(function (resp) {
            resolve(resp.hits.total);
        });
    }, function (err) {
        console.trace(err.message);
    });
}

function getNumberOfN() {
    return new Promise(resolve => {
        client.search({
            index: config.elasticIndex + "-n0",
            body: {
                query: {
                    match_all: {}
                }
            }
        }).then(function (resp) {
            resolve(resp.hits.total);
        });
    }, function (err) {
        console.trace(err.message);
    });
}

async function getIdsArr() {
    const idsArr = [];
    // let nemberOfDocs = await getNumberOfDocs();
    for (let i = 0; config.numberOfDocToGet > i; i++) {
        // let randomIter = Math.floor((Math.random() * 2) + 1);
        // let randomChar = randomIter == 1 ? 'a' : 'b';

        let randomId = Math.floor((Math.random() * nemberOfDocs) + 1);
        if (idsArr.indexOf(randomId) < 0)
            idsArr.push(randomId);
    }
    return idsArr;
}

async function getN(id) {
    const response = await client.get({
        index: config.elasticIndex + "-n0",
        type: 'doc',
        id: id,
        _source: ["relation"]
    });
    return response;
}

async function maxQuery(nid) {
    const response = await client.search({
        index: config.elasticIndex + "-p0",
        _source: false,
        body: {
            size: 0,
            query: {
                bool: {
                    filter: {
                        "term": {"nId": nid}
                    }
                }
            }
        }
    });
    return response;
}

async function arrayQuery(idsArr) {
    const response = await client.search({
        index: config.elasticIndex + "-p0",
        _source: false,
        body: {
            size: 0,
            query: {
                "terms": {"entityId": idsArr}
            }
        }
    });
    return response;
}

function getRandomNetId(max) {
    return Math.floor((Math.random() * max) + 0).toString()
}

async function indexResult(body) {
    const response = await client.index({
        index: 'gq_result',
        type: '_doc',
        body: body
    });
    return response;
}

async function main() {
    // let numberOfN = await getNumberOfN();
    let numberOfN = 10;

    console.time('maxQuery');
    const t0 = Date.now();

    const terms = await maxQuery(getRandomNetId(numberOfN));
    console.timeEnd('maxQuery');
    var timeMax = Date.now() - t0;

    console.log('maxQuery result:', terms.hits.total);
    console.log("timeMax:", timeMax)
    console.log("\n")

    await indexResult({type: "maxQuery", performance: timeMax, meta_document_returns: terms.hits.total})

    const n = await getN(getRandomNetId(numberOfN));

    console.time('arrayQuery');
    var t1 = Date.now();

    const result = await arrayQuery(n._source.relation);

    console.timeEnd('arrayQuery');
    var timeArray = Date.now() - t1;


    console.log('arrayQuery result:', result.hits.total);
    console.log("timeArray:", timeArray)
    await indexResult({type: "arrayQuery", performance: timeArray, meta_document_returns: result.hits.total})


}

main();