const nats = require('node-nats-streaming');

function connect(connectString) {
    return new Promise((resolve, reject) => {
        const stan = nats.connect('cyberway', 'queue-sync', { url: connectString });

        stan.on('connect', () => {
            resolve(stan);
        });

        stan.on('error', err => {
            console.error('STAN Error:', err);
            reject(err);
        });
    });
}

function getLastSequence(stan) {
    return new Promise((resolve, reject) => {
        const options = stan.subscriptionOptions();
        options.setStartWithLastReceived();
        options.setMaxInFlight(10);

        const subscription = stan.subscribe('Blocks', options);

        let seq;

        subscription.on('message', msg => {
            seq = msg.getSequence();
            subscription.unsubscribe();
        });

        subscription.on('unsubscribed', () => {
            resolve(seq);
        });
    });
}

function getBlockAtSeq(stan, seq) {
    return new Promise((resolve, reject) => {
        const options = stan.subscriptionOptions();
        options.setStartAtSequence(seq);
        options.setMaxInFlight(10);

        const subscription = stan.subscribe('Blocks', options);

        subscription.on('message', msg => {
            subscription.unsubscribe();

            resolve(
                new Promise(resolve => {
                    if (seq !== msg.getSequence()) {
                        throw new Error('Seq mismatched');
                    }

                    const block = getBlock(msg);

                    if (block.msg_type !== 'AcceptBlock') {
                        throw new Error('Not AcceptBlock');
                    }

                    resolve(block);
                })
            );
        });
    });
}

async function findBlockSeq(stan, block, options) {
    const lastSeq = await getLastSequence(stan);

    return await seekBlock(stan, 1, lastSeq, block, options);
}

async function seekBlock(stan, from, before, findBlock, options = {}) {
    const { compareByBlockNum = false, messageType = 'CommitBlock' } = options;

    const width = before - from;

    console.log(`SEEK: [${from}, ${before}) width: ${width}`);

    if (width < 100) {
        return await findInRange(stan, from, before, [messageType], block => {
            if (compareByBlockNum) {
                return block.block_num === findBlock.block_num;
            }

            return block.id === findBlock.id;
        });
    } else {
        const halfSeq = Math.floor(from + width / 2);

        let findInLeftPart = false;
        let findInRightPart = false;
        let blockFound = null;

        await findInRange(stan, halfSeq, before, [messageType], block => {
            const blockNum = block.block_num;

            if (compareByBlockNum) {
                if (block.block_num === findBlock.block_num) {
                    blockFound = block;
                    return true;
                }
            } else {
                if (block.id === findBlock.id) {
                    blockFound = block;
                    return true;
                }
            }

            if (blockNum > findBlock.block_num) {
                findInLeftPart = true;
            } else {
                findInRightPart = true;
            }

            return true;
        });

        if (blockFound) {
            return blockFound;
        }

        if (findInLeftPart) {
            return await seekBlock(stan, from, halfSeq, findBlock, options);
        } else if (findInRightPart) {
            return await seekBlock(stan, halfSeq, before, findBlock, options);
        }

        throw new Error('Invariant');
    }
}

async function findInRange(stan, from, before, actions, callback) {
    return new Promise((resolve, reject) => {
        const options = stan.subscriptionOptions();
        options.setStartAtSequence(from);
        options.setMaxInFlight(10);

        const subscription = stan.subscribe('Blocks', options);

        subscription.on('message', msg => {
            const block = getBlock(msg);

            if (block.sequence >= before) {
                subscription.unsubscribe();
                reject(new Error('Range is end'));
                return;
            }

            if (actions.includes(block.msg_type)) {
                if (callback(block)) {
                    subscription.unsubscribe();
                    resolve(block);
                }
            }
        });
    });
}

function getBlock(msg) {
    const block = JSON.parse(msg.getData());
    block.sequence = msg.getSequence();
    return block;
}

async function findLastIrreversibleBlockBefore(stan, beforeSeq) {
    return await find(stan, beforeSeq, data => data.msg_type === 'CommitBlock');
}

async function findLastIrreversibleBlockAcceptBefore(stan, block) {
    return await find(
        stan,
        block.sequence,
        data => data.msg_type === 'AcceptBlock' && data.id === block.id
    );
}

async function find(stan, beforeSeq, predicate) {
    let shift = 0;

    let lastBlockCommit = null;

    while (!lastBlockCommit) {
        const start = Math.max(1, beforeSeq - 100 * (shift + 1));
        const end = beforeSeq - 100 * shift;

        if (start >= end) {
            throw new Error('Block not found');
        }

        lastBlockCommit = await new Promise(resolve => {
            const options = stan.subscriptionOptions();
            options.setStartAtSequence(start);
            options.setMaxInFlight(10);

            let lastBlockCommit = null;

            const subscription = stan.subscribe('Blocks', options);

            subscription.on('message', msg => {
                const seq = msg.getSequence();
                const data = JSON.parse(msg.getData());
                data.sequence = seq;

                if (seq >= end) {
                    subscription.unsubscribe();
                    resolve(lastBlockCommit);
                    return;
                }

                if (predicate(data)) {
                    lastBlockCommit = data;
                }
            });
        });

        shift++;
    }

    return lastBlockCommit;
}

function cutBlock(block) {
    delete block.msg_channel;
    delete block.active_schedule;
    delete block.next_schedule;
    delete block.trxs;
    delete block.scheduled_slot;
    delete block.scheduled_shuffle_slot;
    delete block.dpos_irreversible_blocknum;
    delete block.block_slot;
    delete block.next_block_time;

    return block;
}

module.exports = {
    connect,
    getLastSequence,
    getBlockAtSeq,
    findBlockSeq,
    findLastIrreversibleBlockBefore,
    findLastIrreversibleBlockAcceptBefore,
    cutBlock,
};
