const {
    connect,
    cutBlock,
    getBlockAtSeq,
    findBlockSeq,
    findLastIrreversibleBlockBefore,
    findLastIrreversibleBlockAcceptBefore,
} = require('./natsUtils');

const { nodeFrom, nodeTo, blockSubscribe } = require('../data.json');

async function main() {
    const stanFrom = await connect(nodeFrom);
    const stan = await connect(nodeTo);

    console.log('Nats connected\n');

    try {
        await run(stanFrom, stan);
    } catch (err) {
        console.error('Run error:', err);
    }

    stanFrom.close();
    stan.close();
}

async function run(stanFrom, stan) {
    const block = await getBlockAtSeq(stanFrom, blockSubscribe.lastBlockSequence);

    if (block.block_num !== blockSubscribe.lastBlockNum) {
        throw new Error('Invalid block');
    }

    console.log('block:', cutBlock(block));

    const irrBlock = await findLastIrreversibleBlockBefore(
        stanFrom,
        blockSubscribe.lastBlockSequence
    );

    console.log('irreversible block:', cutBlock(irrBlock));

    stanFrom.close();

    const found = await findBlockSeq(stan, irrBlock);

    console.log('Irr block found:', cutBlock(found));

    const foundAccept = await findLastIrreversibleBlockAcceptBefore(stan, found);

    console.log('Accept Irr block found:', cutBlock(foundAccept));

    console.log('New sequence:', foundAccept.sequence);

    stan.close();
}

main().catch(err => {
    console.error('Global error:', err);
});
