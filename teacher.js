// profesor.js (versión para múltiples estudiantes)
const amqp = require('amqplib');

// Reemplaza 'localhost' con la IP de la máquina donde corre RabbitMQ
const RABBITMQ_URL = 'amqp://localhost';

// --- DATOS DE LA CLASE ---
const ESTUDIANTES = [
        'estudiante.leidy',
        'estudiante.vanessa',
        'estudiante.yuly',
        'estudiante.angie',
        'estudiante.tomas',
        'estudiante.river',
        'estudiante.juanJose',
        'estudiante.erik',
        'estudiante.diego',
        'estudiante.luis',
        'estudiante.johan',
        'estudiante.johnny',
        'estudiante.brayan',
        'estudiante.juanMiguel',
        'estudiante.samuel',
        'estudiante.william',
        'estudiante.deisy',
        'estudiante.juanManuel',
        'estudiante.eval',
        'estudiante.francisco',
        'estudiante.danilo',
        'estudiante.juanCarlos',
        'estudiante.diego',
        'estudiante.anderson',
        'estudiante.sara',
        'estudiante.cristhian',
        'estudiante.javier',
        'estudiante.antonio',
        'estudiante.sebastián',
        'estudiante.dylan',
        'estudiante.sebastian',
        'estudiante.juanFernando',
        'estudiante.valery',
        'estudiante.jacobo',
        'estudiante.jeronimo',
        'estudiante.dania',
        'estudiante.manuel',
        'estudiante.juanVizuette',
        'estudiante.evelyn'
    ];

const PREGUNTAS = [
  "¿Quién pintó 'El nacimiento de Venus'?",
  "¿Cuál es la capital de Angola?",
  "¿En qué país se encuentra la Gran Barrera de Coral?",
  "¿Cuál es la capital de Brasil?",
  "¿Cuál es la capital de Austria?",
  "¿Qué empresa desarrolló el primer microprocesador, el Intel 4004?",
  "¿En qué país se encuentra el Salto Ángel, la cascada más alta del mundo?",
  "¿Cuál es el país más grande del mundo por superficie?",
  "¿Cuál es la capital de Croacia?",
  "¿Qué significan las siglas 'CPU' en un ordenador?",
  "¿Qué pintor español creó la famosa obra 'Guernica'?",
  "¿Cuál es la capital de Jamaica?",
  "¿Qué dos países comparten la isla de La Española?",
  "¿Cuál es la capital de Colombia?",
  "¿Cuál es el océano más grande del mundo?",
  "¿Quién compuso la 'Novena Sinfonía'?",
  "¿Cuál es la capital de Indonesia?",
  "¿Cuál es el mar con la salinidad más alta del mundo?",
  "¿Quién es el autor de 'Romeo y Julieta'?",
  "¿Cuál es la capital de Corea del Sur?",
  "¿Cuál es el ingrediente principal del guacamole?",
  "¿Qué red social fue fundada por Mark Zuckerberg?",
  "¿Qué es la memoria 'RAM' en un dispositivo?",
  "¿Cuál es la capital de Cuba?",
  "¿Qué sistema de comunicación utiliza puntos y rayas?",
  "¿Qué significa la sigla 'HTML'?",
  "¿En qué continente se encuentra Siria?",
  "¿Cuál es la capital de Filipinas?",
  "¿Cuál es la capital de Kenia?",
  "¿Qué es un 'firewall' en informática?",
  "¿Cuál es el país más poblado del mundo actualmente?",
  "¿Qué es 'la nube' en informática?",
  "¿Cuál es la capital de Venezuela?",
  "¿En qué año lanzó Apple el primer iPhone?",
  "¿Cuál es la capital de Albania?",
  "¿Quién escribió la novela distópica '1984'?",
  "¿En qué año cayó el Muro de Berlín?",
  "¿Cuál es la capital de Guatemala?",
  "¿Cuál fue el título de la última faraona del Antiguo Egipto?",
  "¿Quién fue el primer presidente de Sudáfrica elegido democráticamente?",
  "¿Cuál es la capital de Tailandia?",
  "¿En qué país se encuentra la península de Yucatán?",
  "¿Cuál es la capital de Uruguay?",
  "¿Qué es un conjunto de islas agrupadas?",
  "¿Quién fue el primer ser humano en viajar al espacio exterior?",
  "¿Cuál es la capital de España?",
  "¿En qué deporte profesional compite Rafael Nadal?",
  "¿Cuál es la capital de Australia?",
  "¿Quién escribió 'El principito'?",
  "¿Qué científico polaco fue pionera en el estudio de la radiactividad?"
];

// const PREGUNTAS = [
//   "¿Quién escribió 'Cien años de soledad'?",
//   "¿Cuál es la capital de Alemania?",
//   "¿Quién fue Tim Berners-Lee?",
//   "¿En qué año se fundó Microsoft?",
//   "¿Qué es un 'bug' en el contexto del software?",
//   "¿Cuál es la capital de Arabia Saudita?",
//   "¿Cuál es la capital de China?",
//   "¿Qué sistema operativo fue creado por Linus Torvalds?",
//   "¿Quién fue Ada Lovelace?",
//   "¿Quién compuso la obra musical 'Las cuatro estaciones'?",
//   "¿Cuál es la capital de El Salvador?",
//   "¿Qué significan las siglas 'URL'?",
//   "¿Cuál es la capital de Suecia?",
//   "¿En qué país se encuentran las pirámides de Giza?",
//   "¿Qué océano bordea la costa oeste de Estados Unidos?",
//   "¿Quién pintó la Mona Lisa?",
//   "¿Qué cadena montañosa separa la península ibérica del resto de Europa?",
//   "¿Qué constante matemática representa la relación entre la circunferencia y el diámetro?",
//   "¿Quién es considerado el 'padre de la computación'?",
//   "¿Cuál es la capital de Argentina?",
//   "¿Cuál es la capital de Islandia?",
//   "¿Cuál es el lago navegable más alto del mundo?",
//   "¿Qué compañía de videojuegos creó al personaje 'Mario'?",
//   "¿Cuál es la capital de Países Bajos?",
//   "¿Cuál es la capital de Rusia?",
//   "¿En qué continente se encuentra la cordillera de los Andes?",
//   "¿Cuál es la capital de Perú?",
//   "¿Cuál es la capital de Vietnam?",
//   "¿Quién escribió 'El diario de Ana Frank'?",
//   "¿En qué año llegó el ser humano a la luna por primera vez?",
//   "¿Cómo se llama la galaxia en la que se encuentra nuestro sistema solar?",
//   "¿Cuál es la capital de Panamá?",
//   "¿Cuál es la capital de Paraguay?",
//   "¿Cuál es el desierto cálido más grande del mundo?",
//   "¿Cuál es la capital de Francia?",
//   "¿Cuál es el hueso más largo del cuerpo humano?",
//   "¿Qué significa 'Wi-Fi'?",
//   "¿Cuál es la capital de Polonia?",
//   "¿Cuál es la capital de Canadá?",
//   "¿Cuál es la capital de Andorra?",
//   "¿Cuál es la capital de Dinamarca?",
//   "¿Quién dirigió la película 'Pulp Fiction'?",
//   "¿En qué año comenzó la Primera Guerra Mundial?",
//   "¿Cuál es la capital de Bélgica?",
//   "¿Cómo se llama el proceso de las plantas para convertir luz en energía?",
//   "¿Cuál es la capital de Portugal?",
//   "¿Qué elemento químico tiene el símbolo 'Au'?",
//   "¿Cuál es la capital de Italia?",
//   "¿Cuál es la capital de India?",
//   "¿Cuál es el mar interior más grande del mundo?"
// ];
// const PREGUNTAS = [
//   "¿Quién desarrolló la teoría de la relatividad general?",
//   "¿Qué artefacto antiguo permitió descifrar los jeroglíficos egipcios?",
//   "¿Quién pintó 'La noche estrellada'?",
//   "¿Cuál es el río más largo del mundo?",
//   "¿Cuál es la capital de Chile?",
//   "¿En qué país se encuentra el volcán Etna?",
//   "¿Cuál es el animal terrestre más rápido?",
//   "¿Cuál es la capital de Afganistán?",
//   "¿En qué país se inventó la pólvora?",
//   "¿Cuál es la capital de Argelia?",
//   "¿Qué proyecto científico internacional logró secuenciar el ADN humano?",
//   "¿Quién fue el primer presidente de los Estados Unidos?",
//   "¿Cuál es la montaña más alta del mundo?",
//   "¿Quién pintó el techo de la Capilla Sixtina?",
//   "¿Qué palabra japonesa designa una ola gigante producida por un maremoto?",
//   "¿En qué ciudad de Estados Unidos se originó el género musical del jazz?",
//   "¿Qué festividad cristiana conmemora la resurrección de Jesús?",
//   "¿Cuál es el órgano más grande del cuerpo humano?",
//   "¿De qué país es originaria la práctica de la acupuntura?",
//   "¿Cuál fue la capital del Imperio Bizantino?",
//   "¿Cuál es la capital de Bolivia?",
//   "¿En qué hemisferio se observa principalmente la aurora austral?",
//   "¿Cuál es la capital de Israel?",
//   "¿Cómo se llama la campana del reloj del Palacio de Westminster?",
//   "¿A quién se le atribuye la invención del teléfono?",
//   "¿Cuál es la capital de Nicaragua?",
//   "¿Quién descubrió la penicilina?",
//   "¿En qué desierto se encuentra el Valle de la Muerte?",
//   "¿Cuál es la capital de Finlandia?",
//   "¿Cuál es la ciudad más austral del mundo?",
//   "¿Cuál es la capital de Grecia?",
//   "¿Cuál es la capital de Noruega?",
//   "¿Cuál es la capital de Egipto?",
//   "¿Cuál es el mamífero más grande del mundo?",
//   "¿Qué movimiento artístico fundaron Pablo Picasso y Georges Braque?",
//   "¿Qué río forma la frontera entre México y Estados Unidos?",
//   "¿Cuántos bits conforman un byte?",
//   "¿Cuál es la capital de México?",
//   "¿Cuál es la capital ejecutiva de Sudáfrica?",
//   "¿En qué país se originaron los Juegos Olímpicos Antiguos?",
//   "¿Qué es un 'bug' en el contexto del software?",
//   "¿Cuál es la capital de Hungría?",
//   "¿Cuál es la capital de Japón?",
//   "¿Cuál es la capital de Costa Rica?",
//   "¿Cuál es la capital de Marruecos?",
//   "¿Qué mide la escala de Richter?",
//   "¿Cuál es la moneda oficial de Japón?",
//   "¿Cuál es la capital de Estados Unidos?",
//   "¿Quién escribió la novela 'Orgullo y Prejuicio'?",
//   "¿Qué empresa de tecnología tiene su sede principal en Cupertino, California?"
// ];
// const PREGUNTAS = [
//   "¿Qué líder religioso inició la Reforma Protestante en el siglo XVI?",
//   "¿Cuál es la fórmula química del agua?",
//   "¿Quién escribió la epopeya 'La Odisea'?",
//   "¿Cuál es la capital de Ecuador?",
//   "¿A qué heroína francesa se conoce como la 'Doncella de Orleans'?",
//   "¿De qué país es originario el plato conocido como sushi?",
//   "¿Qué red social se caracteriza por mensajes cortos de hasta 280 caracteres?",
//   "¿Cómo se llama el asistente virtual desarrollado por Amazon?",
//   "¿Qué lenguaje de programación fue creado por Guido van Rossum?",
//   "¿Cuál es la isla más grande del mundo?",
//   "¿Cuál es la capital de Suiza?",
//   "¿En qué juego de mesa se utiliza el término 'jaque mate'?",
//   "¿Quién fue el fundador del Imperio Mongol?",
//   "¿Cuál es la capital de Turquía?",
//   "¿Cuál es la capital de Irlanda?",
//   "¿Qué estrecho separa España de Marruecos?",
//   "¿Cuál es la capital de Reino Unido?",
//   "¿Cuál es la selva tropical más grande del mundo?",
//   "¿En qué país se encuentra el Gran Cañón del Colorado?",
//   "¿Qué servicio de correo electrónico fue lanzado por Google en 2004?",
//   "¿Qué río pasa por la ciudad de Roma?",
//   "¿Cuál es la capital de Irán?",
//   "¿Cuál es la unidad de medida para la velocidad del procesador de un ordenador?",
//   "¿Cuál es el océano más pequeño del mundo?",
//   "¿Cómo se llama la tecla en la parte superior izquierda de la mayoría de los teclados?",
//   "¿Cómo se llama el pigmento verde que da color a las plantas?",
//   "¿Cuál es la capital de Irak?",
//   "¿Quién compuso la banda sonora original de la saga 'Star Wars'?",
//   "¿Qué significa 'PDF'?",
//   "¿Qué planeta es conocido como el 'planeta rojo'?",
//   "¿En qué ciudad se encuentra el Coliseo Romano?",
//   "¿Qué escultor francés creó la obra 'El Pensador'?",
//   "¿Cuál es la capital de República Dominicana?",
//   "¿Cuál es el libro sagrado del Islam?",
//   "¿Qué ciencia estudia los fósiles?",
//   "¿Cada cuántos años ocurre un año bisiesto?",
//   "¿Cuál es la capital de Nueva Zelanda?",
//   "¿Quién es el dios del trueno en la mitología nórdica?",
//   "¿En qué país se encuentra el monte Kilimanjaro?",
//   "¿De qué insecto proviene la fibra natural de la seda?",
//   "¿Qué país es conocido como la 'tierra del sol naciente'?",
//   "¿Cómo se llama el teorema matemático sobre los triángulos rectángulos?",
//   "¿Con qué apodo se conoce la salida del Reino Unido de la Unión Europea?",
//   "¿Cómo se llama al software malicioso diseñado para dañar un sistema?",
//   "¿Cuál es el instrumento de viento-metal más grave de una orquesta?",
//   "¿Cuál es la capital nacional más alta del mundo?",
//   "¿Cuál es la estrella más cercana a la Tierra?",
//   "¿En qué país se encuentra la Falla de San Andrés?",
//   "¿Cuántos versos tiene un soneto clásico?",
//   "¿Cuál fue el primer navegador web gráfico popular?"
// ];
// -------------------------

async function enviarPreguntas() {
    if (ESTUDIANTES.length > PREGUNTAS.length) {
        console.error("Error: No hay suficientes preguntas para todos los estudiantes.");
        return;
    }

    let connection;
    try {
        connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        const exchange = 'aula_exchange';
        await channel.assertExchange(exchange, 'direct', { durable: false });

        const replyQueue = await channel.assertQueue('', { exclusive: true });
        console.log('📬 El profesor está esperando respuestas en la cola:', replyQueue.queue);

        const correlationMap = new Map();
        let respuestasRecibidas = 0;

        channel.consume(replyQueue.queue, (msg) => {
            const estudianteId = correlationMap.get(msg.properties.correlationId);
            if (estudianteId) {
                const respuesta = msg.content.toString();
                // --- INICIO DEL CAMBIO ---
                // Mensaje mejorado para manejar respuestas de tiempo agotado
                if (respuesta === "¡Tiempo agotado!") {
                    console.log(`[PROFESOR] ⌛️ ${estudianteId} no respondió a tiempo.`);
                } else {
                    console.log(`[PROFESOR] 🎓 Respuesta recibida de ${estudianteId}: "${respuesta}"`);
                }
                // --- FIN DEL CAMBIO ---
                
                respuestasRecibidas++;
                correlationMap.delete(msg.properties.correlationId); // Limpiar el mapa

                if (respuestasRecibidas === ESTUDIANTES.length) {
                    console.log("\n✅ ¡Todos los estudiantes han respondido o se quedaron sin tiempo! Cerrando.");
                    setTimeout(() => {
                        connection.close();
                        process.exit(0);
                    }, 500);
                }
            }
        }, { noAck: true });

        console.log("\n--- Enviando preguntas individuales ---\n");
        ESTUDIANTES.forEach((estudianteId, index) => {
            const pregunta = PREGUNTAS[index];
            const correlationId = generateUuid();
            correlationMap.set(correlationId, estudianteId);
            channel.publish(exchange, estudianteId, Buffer.from(pregunta), {
                correlationId: correlationId,
                replyTo: replyQueue.queue
            });
            console.log(`[PROFESOR] 📨 Enviando a ${estudianteId}: "${pregunta}"`);
        });
    } catch (error) {
        console.error("Error:", error);
        if (connection) await connection.close();
    }
}

function generateUuid() {
    return Math.random().toString(36).substring(2, 15);
}

enviarPreguntas();