const readline = require('readline');

// CONFIGURACIÓN
const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';
const MI_ID = 'estudiante.alvarado'; // <--- CADA ESTUDIANTE CAMBIA ESTO
const EXCHANGE = 'aula_exchange';

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

async function iniciarEstudiante() {
    try {
        const amqplib = require('amqplib');
        const connection = await amqplib.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        // 1. Declarar el exchange (debe coincidir con el del profesor)
        await channel.assertExchange(EXCHANGE, 'direct', { durable: false });

        // 2. Crear una cola propia para el estudiante
        // Usamos el MI_ID como nombre de la cola para que el Exchange Direct sepa a dónde enviar
        const q = await channel.assertQueue(MI_ID, { durable: false, autoDelete: true });
        
        // 3. Vincular la cola con su ID personal (Routing Key)
        await channel.bindQueue(q.queue, EXCHANGE, MI_ID);

        console.log(`🎓 [${MI_ID}] Conectado. Esperando pregunta del profesor...`);

        // 4. Escuchar la pregunta
        channel.consume(q.queue, (msg) => {
            const pregunta = msg.content.toString();
            const { replyTo, correlationId } = msg.properties;

            console.log(`\n[PROFESOR DICE]: ${pregunta}`);

            // 5. Pedir la respuesta por consola
            rl.question('Escribe tu respuesta: ', (respuesta) => {
                // 6. Enviar la respuesta a la cola que el profesor indicó en 'replyTo'
                channel.sendToQueue(replyTo, Buffer.from(respuesta), {
                    correlationId: correlationId
                });

                console.log(`[SISTEMA] Respuesta enviada. ¡Suerte!`);
            });

        }, { noAck: true });

        const exchangeFanout = 'notificaciones_ganador';
        await channel.assertExchange(exchangeFanout, 'fanout', { durable: false });

        const qFanout = await channel.assertQueue('', { exclusive: true });
        await channel.bindQueue(qFanout.queue, exchangeFanout, '');

        // Escuchar el anuncio del ganador
        channel.consume(qFanout.queue, (msg) => {
            console.log(`\n📢 [NOTIFICACIÓN GLOBAL]: ${msg.content.toString()}`);
            process.exit(0);
        }, { noAck: true });

    } catch (error) {
        console.error("Error en el estudiante:", error);
    }
}

iniciarEstudiante();