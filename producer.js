// profesor.js
const amqp = require('amqplib');

// Reemplaza 'localhost' con la IP de la máquina donde corre RabbitMQ
// const RABBITMQ_URL = 'amqp://192.168.20.242'; 
const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';

async function enviarMensaje() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        const exchange = 'aula_exchange';
        const estudianteId = 'estudiante.sextoucc'; // A quién va dirigido el mensaje
        const pregunta = '¿Cuál es la capital de Colombia?';

        await channel.assertExchange(exchange, 'direct', { durable: false });

        // 1. Crear una cola exclusiva y anónima para las respuestas
        const replyQueue = await channel.assertQueue('', { exclusive: true });
        console.log('📬 Esperando respuestas en la cola:', replyQueue.queue);

        // 2. Generar un ID de correlación único
        const correlationId = generateUuid();

        console.log(`[PROFESOR] 📨 Enviando pregunta a ${estudianteId}: "${pregunta}"`);

        // 3. Publicar el mensaje con 'replyTo' y 'correlationId'
        channel.publish(exchange, estudianteId, Buffer.from(pregunta), {
            correlationId: correlationId,
            replyTo: replyQueue.queue
        });

        // 4. Escuchar en la cola de respuestas
        channel.consume(replyQueue.queue, (msg) => {
            // Asegurarse de que la respuesta corresponde a nuestra pregunta
            if (msg.properties.correlationId === correlationId) {
                console.log(`[PROFESOR] 🎓 Respuesta recibida de ${estudianteId}: "${msg.content.toString()}"`);
                
                // Cerrar la conexión después de recibir la respuesta
                setTimeout(() => {
                    connection.close();
                    process.exit(0);
                }, 500);
            }
        }, { noAck: true });

    } catch (error) {
        console.error("Error:", error);
    }
}

// Función simple para generar un ID único
function generateUuid() {
    return Math.random().toString() + Math.random().toString() + Math.random().toString();
}

enviarMensaje();