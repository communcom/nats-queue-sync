const { connect, cutBlock, findBlockSeq } = require('./natsUtils');

const { nodeTo, blockSubscribe } = require('../data.json');

async function main() {
    const stan = await connect(nodeTo);

    console.log('Nats connected\n');

    try {
        await run(stan);
    } catch (err) {
        console.error('Run error:', err);
    }

    stan.close();
}

async function run(stan) {
    const found = await findBlockSeq(
        stan,
        {
            block_num: blockSubscribe.lastBlockNum,
        },
        {
            messageType: 'AcceptBlock',
            compareByBlockNum: true,
        }
    );

    console.log('Block found:', cutBlock(found));

    stan.close();
}

main().catch(err => {
    console.error('Global error:', err);
});
