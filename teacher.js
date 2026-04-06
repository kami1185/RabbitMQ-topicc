// profesor.js (versión para múltiples estudiantes)
const amqp = require('amqplib');

// Reemplaza 'localhost' con la IP de la máquina donde corre RabbitMQ
// const RABBITMQ_URL = 'amqp://10.162.38.140';
const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';

// --- DATOS DE LA CLASE ---
const ESTUDIANTES = [
        'estudiante.alvarado',
        'estudiante.arias',
        'estudiante.arevalo',
        'estudiante.bastidas',
        'estudiante.burbanoJose',
        'estudiante.burbanoValentina', // Nota: Hay dos Burbano, RabbitMQ los tratará como dos consumidores si ambos se conectan
        'estudiante.cajigas',
        'estudiante.calzada',
        'estudiante.castillo',
        'estudiante.cordoba',
        'estudiante.galvez',
        'estudiante.garcia',
        'estudiante.gomez',
        'estudiante.guerrero',
        'estudiante.limas',
        'estudiante.lucero',
        'estudiante.luna',
        'estudiante.mallama',
        'estudiante.martinez',
        'estudiante.martinez',
        'estudiante.maya',
        'estudiante.meneses',
        'estudiante.mideros',
        'estudiante.montero',
        'estudiante.mora',
        'estudiante.moreno',
        'estudiante.noguera',
        'estudiante.ojeda',
        'estudiante.ortega',
        'estudiante.paredes',
        'estudiante.pulsara',
        'estudiante.ramirez',
        'estudiante.solarte',
        'estudiante.valencia',
        'estudiante.villota',
        'estudiante.wagimin',
        'estudiante.yepes',
        'estudiante.zambrano'
    ];

const PREGUNTAS = [
  "¿Quién escribió 'La Iliada'?",
  "¿Cuál es la capital de Lituania?",
  "¿En qué año comenzó la Primera Guerra Mundial?",
  "¿En qué año se fundó Oracle?",
  "¿Cuál es la capital de Arabia Saudita?",
  "¿Cuál es la capital de China?",
  "¿Qué sistema operativo fue creado por Linus Torvalds?",
  "¿Cuál es la capital de Andorra?",
  "¿Qué elemento químico tiene el símbolo 'Au'?",
  "¿Cuál es la capital de El Salvador?",
  "¿Qué significan las siglas 'URL'?",
  "¿Cuál es la capital de Suecia?",
  "¿En qué país se encuentran las pirámides de Giza?",
  "¿Qué océano bordea la costa oeste de Estados Unidos?",
  "¿Quién pintó la Mona Lisa?",
  "¿Qué cadena montañosa separa la península ibérica del resto de Europa?",
  "¿Qué constante matemática representa la relación entre la circunferencia y el diámetro?",
  "¿Quién es considerado el 'padre de la computación'?",
  "¿Cuál es la capital de Honduras?",
  "¿Cuál es la capital de Islandia?",
  "¿Cuál es el lago navegable más alto del mundo?",
  "¿Qué compañía de videojuegos creó al personaje 'Mario'?",
  "¿Cuál es la capital de Países Bajos?",
  "¿Cuál es la capital de Rusia?",
  "¿En qué continente se encuentra la cordillera de los Andes?",
  "¿Cuál es la capital de Perú?",
  "¿Cuál es la capital de Vietnam?",
  "¿Quién escribió 'El diario de Ana Frank'?",
  "¿En qué año llegó el ser humano a la luna por primera vez?",
  "¿Cómo se llama la galaxia en la que se encuentra nuestro sistema solar?",
  "¿Cuál es la capital de Panamá?",
  "¿Cuál es la capital de Paraguay?",
  "¿Cuál es el desierto cálido más grande del mundo?",
  "¿Cuál es la capital de Letonia?",
  "¿Cuál es el hueso más largo del cuerpo humano?",
  "¿Qué significa 'Wi-Fi'?",
  "¿Cuál es la capital de Polonia?",
  "¿Cuál es la capital de Canadá?",
  "¿Cuál es la capital de Andorra?",
  "¿Cuál es la capital de Dinamarca?",
  "¿Quién dirigió la película 'Pulp Fiction'?",
  "¿En qué año comenzó la Primera Guerra Mundial?",
  "¿Cuál es la capital de Bélgica?",
  "¿Cómo se llama el proceso de las plantas para convertir luz en energía?",
  "¿Cuál es la capital de Portugal?",
  "¿Qué elemento químico tiene el símbolo 'Au'?",
  "¿Cuál es la capital de Indonesia?",
  "¿Cuál es la capital de India?",
  "¿Cuál es el mar interior más grande del mundo?"
];

async function enviarPreguntas() {
    let connection;
    try {
        connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        
        const exchangeDirect = 'aula_exchange'; // Para preguntas individuales
        const exchangeFanout = 'notificaciones_ganador'; // Para el anuncio global

        await channel.assertExchange(exchangeDirect, 'direct', { durable: false });
        await channel.assertExchange(exchangeFanout, 'fanout', { durable: false });

        const replyQueue = await channel.assertQueue('', { exclusive: true });
        
        let ganador = null;
        let tiempoGanador = Infinity;
        let respuestasContadas = 0;
        const tiemposInicio = new Map();

        // 1. Escuchar respuestas
        channel.consume(replyQueue.queue, (msg) => {
            const estudianteId = msg.properties.correlationId; // Usaremos el ID como correlación
            const respuesta = msg.content.toString();
            const tiempoFinal = Date.now();
            const tiempoProcesamiento = (tiempoFinal - tiemposInicio.get(estudianteId)) / 1000;

            if (tiempoProcesamiento <= 10) {
                console.log(`[JUEZ] 🎓 ${estudianteId} respondió en ${tiempoProcesamiento}s: "${respuesta}"`);
                
                // Determinar si es el más rápido hasta ahora
                if (tiempoProcesamiento < tiempoGanador) {
                    tiempoGanador = tiempoProcesamiento;
                    ganador = estudianteId;
                }
            } else {
                console.log(`[JUEZ] ⌛ ${estudianteId} respondió fuera de tiempo (${tiempoProcesamiento}s).`);
            }

            respuestasContadas++;
        }, { noAck: true });

        // 2. Enviar preguntas
        console.log("\n--- ¡EL CONCURSO COMIENZA AHORA! (10s para responder) ---\n");
        
        ESTUDIANTES.forEach((estudianteId, index) => {
            const pregunta = PREGUNTAS[index];
            tiemposInicio.set(estudianteId, Date.now());

            channel.publish(exchangeDirect, estudianteId, Buffer.from(pregunta), {
                correlationId: estudianteId,
                replyTo: replyQueue.queue,
                expiration: '10000' // RabbitMQ descarta el mensaje si no se consume en 10s
            });
        });

        // 3. Esperar 11 segundos para cerrar y anunciar ganador
        setTimeout(() => {
            console.log("\n--- TIEMPO AGOTADO ---");
            const mensajeFinal = ganador 
                ? `🏆 ¡EL GANADOR ES ${ganador.toUpperCase()} con un tiempo de ${tiempoGanador}s!` 
                : "❌ Nadie respondió a tiempo. No hay ganador.";
            
            console.log(mensajeFinal);

            // 4. NOTIFICAR A TODOS (Fanout)
            channel.publish(exchangeFanout, '', Buffer.from(mensajeFinal));

            setTimeout(() => { connection.close(); process.exit(0); }, 2000);
        }, 11000);

    } catch (error) {
        console.error("Error:", error);
    }
}

function generateUuid() {
    return Math.random().toString(36).substring(2, 15);
}

enviarPreguntas();