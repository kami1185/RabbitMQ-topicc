
// Uso: node estudiante.js estudiante.araujo
const amqp = require('amqplib');
const readline = require('readline');

// const RABBITMQ_URL = 'amqp://172.162.0.219';
const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';
const ESTUDIANTE_ID = process.argv[2];
if (!ESTUDIANTE_ID) { console.log('Uso: node estudiante.js estudiante.apellido'); process.exit(1); }

async function main() {
  const connection = await amqp.connect(RABBITMQ_URL);
  const channel = await connection.createChannel();

  await channel.assertExchange('aula_exchange', 'direct', { durable: false });
  await channel.assertExchange('notificaciones_ganador', 'fanout', { durable: false });

  // Cola para MI pregunta (routing key = mi ID)
  const qPregunta = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(qPregunta.queue, 'aula_exchange', ESTUDIANTE_ID);

  // Cola para el anuncio del ganador (fanout: llega a todos)
  const qGanador = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(qGanador.queue, 'notificaciones_ganador', '');

  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  console.log(` ${ESTUDIANTE_ID} conectado. Esperando tu pregunta...`);

  channel.consume(qPregunta.queue, (msg) => {
    const { replyTo, correlationId } = msg.properties;
    console.log(`\n ${msg.content.toString()}`);
    rl.question('Tu respuesta: ', (respuesta) => {
      channel.sendToQueue(replyTo, Buffer.from(respuesta), { correlationId });
      console.log('→ Respuesta enviada. Esperando resultados...');
    });
  }, { noAck: true });

  channel.consume(qGanador.queue, (msg) => {
    console.log(`\n📣 ${msg.content.toString()}`);
    rl.close();
    setTimeout(() => { connection.close(); process.exit(0); }, 500);
  }, { noAck: true });
}

main().catch((err) => { console.error('Error:', err.message); process.exit(1); });