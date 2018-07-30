const config = require("./config");
const elasticsearch = require('elasticsearch');
const client = new elasticsearch.Client({
    host: 'ec2-52-10-42-119.us-west-2.compute.amazonaws.com:9200'
});


function getNumberOfDocs() {
    return new Promise(resolve => {
        client.search({
            index: config.elasticIndex,
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
    let nemberOfDocs = await getNumberOfDocs();
    for (let i = 0; config.numberOfDocToGet > i; i++) {
        let randomIter = Math.floor((Math.random() * 2) + 1);
        let randomChar = randomIter == 1 ? 'a' : 'b';
        let randomId = Math.floor((Math.random() * nemberOfDocs) + 1) + randomChar;
        if (idsArr.indexOf(randomId) < 0)
            idsArr.push(randomId);
    }
    return idsArr;
}

async function mgetQuery(ids) {
    const idsArr = [];

    ids.forEach((id) => {
        idsArr.push({_index: config.elasticIndex, _type: 'doc', _id: id});
    });
    const response = await client.mget({body: {docs: idsArr}});
    return response;
}

async function termsQuery(idsArr) {
    const response = await client.search({
        index: config.elasticIndex,
        body: {
            size: config.numberOfDocToGet,
            query: {
                "terms": {"entityId": idsArr}
            }
        }
    });
    return response;
}

async function main() {
    const ids = await getIdsArr();
    console.time('mgetQuery');
    const mget = await mgetQuery(ids);
    console.timeEnd('mgetQuery');

    console.time('termsQuery');
    const terms = await termsQuery(ids);
    console.timeEnd('termsQuery');

    let x = 5;

}

main();