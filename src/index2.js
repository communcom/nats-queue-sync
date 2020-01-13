const { connect, cutBlock, getBlockAtSeq, findBlockSeq } = require('./natsUtils');

async function main() {
    const stan = await connect('nats://user:lksPIQMCiergnskfgn34t245werasd@localhost:4222');

    console.log('Nats connected\n');

    try {
        await run(stan);
    } catch (err) {
        console.error('Run error:', err);
    }

    stan.close();
}

async function run(stan) {
    const block = await findBlockSeq(stan, {
        id: '002a1d40c4cf12f21125fff02ceed6093830f0c1350be9191352023c20b637aa',
        block_num: 2760000,
    });

    console.log('block:', cutBlock(block));

    for (let i = block.sequence; ; i++) {
        try {
            const res = await getBlockAtSeq(stan, i);
            console.log('res =', cutBlock(res));
            break;
        } catch (err) {}
    }

    console.log('End');
}

main().catch(err => {
    console.error('Global error:', err);
});
