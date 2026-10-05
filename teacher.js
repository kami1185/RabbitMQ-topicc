// profesor.js (versión para múltiples estudiantes)
// const amqp = require('amqplib');

// // Reemplaza 'localhost' con la IP de la máquina donde corre RabbitMQ
// const RABBITMQ_URL = 'amqp://172.162.0.219';
// // const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';

// // --- DATOS DE LA CLASE ---
// const ESTUDIANTES = [
//         'estudiante.araujo',
//         'estudiante.andrade',
//         'estudiante.torres',
//         'estudiante.chacua',
//         'estudiante.mena',
//         'estudiante.gongora', // Nota: Hay dos Burbano, RabbitMQ los tratará como dos consumidores si ambos se conectan
//         'estudiante.patino',
//         'estudiante.narvaez',
//         'estudiante.toro',
//         'estudiante.montilla',
//         'estudiante.gallardo',
//         'estudiante.lopez',
//         'estudiante.criollo',
//         'estudiante.mendoza',
//         'estudiante.rios',
//         'estudiante.mafla',
//         'estudiante.estacio',
//         'estudiante.miranda',
//         'estudiante.montezuma',
//     ];

// const PREGUNTAS = [
//   "¿Quién escribió 'La Iliada'?",
//   "¿Cuál es la capital de Lituania?",
//   "¿En qué año comenzó la Primera Guerra Mundial?",
//   "¿En qué año se fundó Oracle?",
//   "¿Cuál es la capital de Arabia Saudita?",
//   "¿Cuál es la capital de China?",
//   "¿Qué sistema operativo fue creado por Linus Torvalds?",
//   "¿Cuál es la capital de Andorra?",
//   "¿Qué elemento químico tiene el símbolo 'Au'?",
//   "¿Cuál es la capital de El Salvador?",
//   "¿Qué significan las siglas 'URL'?",
//   "¿Cuál es la capital de Suecia?",
//   "¿En qué país se encuentran las pirámides de Giza?",
//   "¿Qué océano bordea la costa oeste de Estados Unidos?",
//   "¿Quién pintó la Mona Lisa?",
//   "¿Qué cadena montañosa separa la península ibérica del resto de Europa?",
//   "¿Qué constante matemática representa la relación entre la circunferencia y el diámetro?",
//   "¿Quién es considerado el 'padre de la computación'?",
//   "¿Cuál es la capital de Honduras?",
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
//   "¿Cuál es la capital de Letonia?",
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
//   "¿Cuál es la capital de Indonesia?",
//   "¿Cuál es la capital de India?",
//   "¿Cuál es el mar interior más grande del mundo?"
// ];

// async function enviarPreguntas() {
//     let connection;
//     try {
//         connection = await amqp.connect(RABBITMQ_URL);
//         const channel = await connection.createChannel();
        
//         const exchangeDirect = 'aula_exchange'; // Para preguntas individuales
//         const exchangeFanout = 'notificaciones_ganador'; // Para el anuncio global

//         await channel.assertExchange(exchangeDirect, 'direct', { durable: false });
//         await channel.assertExchange(exchangeFanout, 'fanout', { durable: false });

//         const replyQueue = await channel.assertQueue('', { exclusive: true });
        
//         let ganador = null;
//         let tiempoGanador = Infinity;
//         let respuestasContadas = 0;
//         const tiemposInicio = new Map();

//         // 1. Escuchar respuestas
//         channel.consume(replyQueue.queue, (msg) => {
//             const estudianteId = msg.properties.correlationId; // Usaremos el ID como correlación
//             const respuesta = msg.content.toString();
//             const tiempoFinal = Date.now();
//             const tiempoProcesamiento = (tiempoFinal - tiemposInicio.get(estudianteId)) / 1000;

//             if (tiempoProcesamiento <= 10) {
//                 console.log(`[JUEZ] 🎓 ${estudianteId} respondió en ${tiempoProcesamiento}s: "${respuesta}"`);
                
//                 // Determinar si es el más rápido hasta ahora
//                 if (tiempoProcesamiento < tiempoGanador) {
//                     tiempoGanador = tiempoProcesamiento;
//                     ganador = estudianteId;
//                 }
//             } else {
//                 console.log(`[JUEZ] ⌛ ${estudianteId} respondió fuera de tiempo (${tiempoProcesamiento}s).`);
//             }

//             respuestasContadas++;
//         }, { noAck: true });

//         // 2. Enviar preguntas
//         console.log("\n--- ¡EL CONCURSO COMIENZA AHORA! (10s para responder) ---\n");
        
//         ESTUDIANTES.forEach((estudianteId, index) => {
//             const pregunta = PREGUNTAS[index];
//             tiemposInicio.set(estudianteId, Date.now());

//             channel.publish(exchangeDirect, estudianteId, Buffer.from(pregunta), {
//                 correlationId: estudianteId,
//                 replyTo: replyQueue.queue,
//                 expiration: '10000' // RabbitMQ descarta el mensaje si no se consume en 10s
//             });
//         });

//         // 3. Esperar 11 segundos para cerrar y anunciar ganador
//         setTimeout(() => {
//             console.log("\n--- TIEMPO AGOTADO ---");
//             const mensajeFinal = ganador 
//                 ? `🏆 ¡EL GANADOR ES ${ganador.toUpperCase()} con un tiempo de ${tiempoGanador}s!` 
//                 : "❌ Nadie respondió a tiempo. No hay ganador.";
            
//             console.log(mensajeFinal);

//             // 4. NOTIFICAR A TODOS (Fanout)
//             channel.publish(exchangeFanout, '', Buffer.from(mensajeFinal));

//             setTimeout(() => { connection.close(); process.exit(0); }, 2000);
//         }, 11000);

//     } catch (error) {
//         console.error("Error:", error);
//     }
// }

// function generateUuid() {
//     return Math.random().toString(36).substring(2, 15);
// }

// enviarPreguntas();



// profesor.js (versión para múltiples estudiantes, con validación de respuestas)
const amqp = require('amqplib');

// Reemplaza con la IP de teacher
//const RABBITMQ_URL = 'amqp://172.162.0.219';
const RABBITMQ_URL = 'amqp://guest:guest@localhost:5672/';

const TIEMPO_LIMITE_SEG = 10;

// --- DATOS DE LA CLASE ---
const ESTUDIANTES = [
  'estudiante.araujo',
  'estudiante.andrade',
  'estudiante.torres',
  'estudiante.chacua',
  'estudiante.mena',
  'estudiante.gongora',
  'estudiante.patino',
  'estudiante.narvaez',
  'estudiante.toro',
  'estudiante.montilla',
  'estudiante.gallardo',
  'estudiante.lopez',
  'estudiante.criollo',
  'estudiante.mendoza',
  'estudiante.rios',
  'estudiante.mafla',
  'estudiante.estacio',
  'estudiante.miranda',
  'estudiante.montezuma',
];

// --- BANCO DE PREGUNTAS: cada pregunta tiene su respuesta correcta y variantes aceptadas ---
// La respuesta NUNCA se envía al estudiante: solo viaja el texto de la pregunta.
const BANCO_PREGUNTAS = [
  // POLÍTICA
  { categoria: 'Política', pregunta: '¿En qué año se promulgó la Constitución Política vigente de Colombia?', respuesta: '1991', alias: ['mil novecientos noventa y uno'] },
  { categoria: 'Política', pregunta: '¿Cuántos miembros permanentes tiene el Consejo de Seguridad de la ONU?', respuesta: '5', alias: ['cinco'] },
  { categoria: 'Política', pregunta: '¿En qué ciudad se encuentra la sede principal de la ONU?', respuesta: 'Nueva York', alias: ['new york', 'ny'] },
  { categoria: 'Política', pregunta: '¿Qué tratado, firmado en 1992, creó formalmente la Unión Europea?', respuesta: 'Tratado de Maastricht', alias: ['maastricht'] },
  { categoria: 'Política', pregunta: '¿Para un período de cuántos años se elige al presidente de Colombia?', respuesta: '4', alias: ['cuatro'] },
  { categoria: 'Política', pregunta: '¿Qué alianza militar, con sede en Bruselas, se conoce por las siglas OTAN en inglés como NATO?', respuesta: 'Organización del Tratado del Atlántico Norte', alias: ['tratado del atlantico norte', 'atlantico norte'] },
  { categoria: 'Política', pregunta: '¿En qué ciudad tiene su sede la Corte Internacional de Justicia?', respuesta: 'La Haya', alias: ['the hague', 'den haag', 'haya'] },

  // GEOGRAFÍA
  { categoria: 'Geografía', pregunta: '¿Cuál es la capital de Australia?', respuesta: 'Canberra', alias: [] },
  { categoria: 'Geografía', pregunta: '¿Cuál es el río más largo de Sudamérica?', respuesta: 'Amazonas', alias: ['rio amazonas'] },
  { categoria: 'Geografía', pregunta: '¿Cuál es el océano más grande del planeta?', respuesta: 'Pacífico', alias: ['oceano pacifico'] },
  { categoria: 'Geografía', pregunta: '¿Qué país es el más poblado de América del Sur?', respuesta: 'Brasil', alias: ['brazil'] },
  { categoria: 'Geografía', pregunta: '¿Qué volcán activo se encuentra junto a la ciudad de Pasto?', respuesta: 'Galeras', alias: ['volcan galeras'] },
  { categoria: 'Geografía', pregunta: '¿Cuál es la capital de Canadá?', respuesta: 'Ottawa', alias: ['otawa'] },
  { categoria: 'Geografía', pregunta: '¿Cuál es la montaña más alta del mundo?', respuesta: 'Everest', alias: ['monte everest'] },

  // PERSONAJES HISTÓRICOS
  { categoria: 'Historia', pregunta: '¿Qué personaje es conocido como "El Libertador"?', respuesta: 'Simón Bolívar', alias: ['bolivar'] },
  { categoria: 'Historia', pregunta: '¿Qué prócer tradujo e imprimió en 1793 la Declaración de los Derechos del Hombre y del Ciudadano?', respuesta: 'Antonio Nariño', alias: ['narino'] },
  { categoria: 'Historia', pregunta: '¿Quién fue la primera persona en ganar dos Premios Nobel?', respuesta: 'Marie Curie', alias: ['curie', 'maria curie', 'marie sklodowska curie'] },
  { categoria: 'Historia', pregunta: '¿Quién lideró la independencia de la India mediante la resistencia no violenta?', respuesta: 'Mahatma Gandhi', alias: ['gandhi', 'mohandas gandhi'] },
  { categoria: 'Historia', pregunta: '¿Quién fue el primer ser humano en caminar sobre la Luna?', respuesta: 'Neil Armstrong', alias: ['armstrong'] },
  { categoria: 'Historia', pregunta: '¿Quién fue el primer presidente de los Estados Unidos?', respuesta: 'George Washington', alias: ['washington'] },
  { categoria: 'Historia', pregunta: '¿Qué navegante llegó a América en 1492 al mando de tres carabelas?', respuesta: 'Cristóbal Colón', alias: ['colon', 'cristoforo colombo', 'colombo', 'columbus'] },
];

// ======================================================================
//  FUNCIONES DE VALIDACIÓN
// ======================================================================

// "  Simón Bolívar! " → "simon bolivar"  (sin tildes, minúsculas, sin puntuación)
function normalizar(texto) {
  return String(texto ?? '')
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// Número mínimo de cambios (letras insertadas, borradas o cambiadas) entre dos textos
function distanciaLevenshtein(a, b) {
  const fila = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    let diagonal = fila[0];
    fila[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const temp = fila[j];
      fila[j] = Math.min(fila[j] + 1, fila[j - 1] + 1, diagonal + (a[i - 1] === b[j - 1] ? 0 : 1));
      diagonal = temp;
    }
  }
  return fila[b.length];
}

// Errores de escritura tolerados según la longitud (los números deben ser exactos)
function erroresPermitidos(texto) {
  if (/^\d+$/.test(texto) || texto.length < 5) return 0;
  return texto.length < 9 ? 1 : 2;
}

/**
 * Valida la respuesta de un estudiante contra la respuesta correcta de su pregunta.
 * Acepta: coincidencia exacta, alias, respuesta dentro de una frase
 * ("la capital es Canberra") y pequeños errores de escritura ("Canbera").
 */
function validarRespuesta(respuestaEstudiante, preguntaAsignada) {
  const r = normalizar(respuestaEstudiante);
  if (!r) return { correcta: false, motivo: 'Respuesta vacía' };

  const aceptadas = [preguntaAsignada.respuesta, ...preguntaAsignada.alias].map(normalizar);

  if (aceptadas.includes(r)) return { correcta: true, motivo: 'Correcta' };

  // La respuesta aparece como palabras completas dentro de una frase
  if (aceptadas.some((a) => ` ${r} `.includes(` ${a} `))) {
    return { correcta: true, motivo: 'Correcta (dentro de la frase)' };
  }

  // Tolerancia a errores de escritura
  if (aceptadas.some((a) => distanciaLevenshtein(r, a) <= erroresPermitidos(a))) {
    return { correcta: true, motivo: 'Correcta (con error de escritura)' };
  }

  return { correcta: false, motivo: 'Incorrecta' };
}

// Asigna una pregunta distinta a cada estudiante (al azar, sin repetir mientras alcancen)
function asignarPreguntas(estudiantes, banco) {
  const mezcladas = [...banco].sort(() => Math.random() - 0.5);
  const asignacion = new Map();
  estudiantes.forEach((id, i) => asignacion.set(id, mezcladas[i % mezcladas.length]));
  return asignacion;
}

// ======================================================================
//  CONCURSO
// ======================================================================

async function enviarPreguntas() {
  let connection;
  try {
    connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    const exchangeDirect = 'aula_exchange';           // Preguntas individuales
    const exchangeFanout = 'notificaciones_ganador';  // Anuncio global

    await channel.assertExchange(exchangeDirect, 'direct', { durable: false });
    await channel.assertExchange(exchangeFanout, 'fanout', { durable: false });

    const replyQueue = await channel.assertQueue('', { exclusive: true });

    const asignacion = asignarPreguntas(ESTUDIANTES, BANCO_PREGUNTAS);
    const tiemposInicio = new Map();
    const resultados = new Map(); // estudianteId → { respuesta, correcta, motivo, tiempo }
    const ausentes = new Set();
    let ganador = null;
    let tiempoGanador = Infinity;
    let concursoCerrado = false;
    let temporizador;

    // Estudiantes sin cola conectada: con mandatory:true el broker devuelve el mensaje
    channel.on('return', (msg) => {
      const id = msg.fields.routingKey;
      ausentes.add(id);
      console.log(`[JUEZ] 🔌 ${id} no está conectado: su pregunta no se pudo entregar.`);
    });

    // 1. Escuchar y VALIDAR respuestas
    channel.consume(replyQueue.queue, (msg) => {
      const estudianteId = msg.properties.correlationId;
      const respuesta = msg.content.toString();

      if (concursoCerrado) return;
      if (!asignacion.has(estudianteId)) {
        console.log(`[JUEZ] ⚠  Respuesta de un estudiante desconocido (${estudianteId}). Ignorada.`);
        return;
      }
      if (resultados.has(estudianteId)) {
        console.log(`[JUEZ] ⚠  ${estudianteId} ya había respondido. Solo cuenta el primer intento.`);
        return;
      }

      const tiempo = (Date.now() - tiemposInicio.get(estudianteId)) / 1000;
      const preguntaAsignada = asignacion.get(estudianteId);

      if (tiempo > TIEMPO_LIMITE_SEG) {
        resultados.set(estudianteId, { respuesta, correcta: false, motivo: 'Fuera de tiempo', tiempo });
        console.log(`[JUEZ] ⌛ ${estudianteId} respondió fuera de tiempo (${tiempo}s).`);
      } else {
        const { correcta, motivo } = validarRespuesta(respuesta, preguntaAsignada);
        resultados.set(estudianteId, { respuesta, correcta, motivo, tiempo });
        console.log(`[JUEZ] ${correcta ? '✔' : '✘'} ${estudianteId} (${tiempo}s): "${respuesta}" → ${motivo}`);

        // Solo las respuestas CORRECTAS compiten por el primer lugar
        if (correcta && tiempo < tiempoGanador) {
          tiempoGanador = tiempo;
          ganador = estudianteId;
        }
      }

      // Si ya respondieron todos los conectados, se cierra sin esperar el tiempo completo
      if (resultados.size + ausentes.size >= ESTUDIANTES.length) {
        clearTimeout(temporizador);
        cerrarConcurso();
      }
    }, { noAck: true });

    // 2. Enviar a cada estudiante SU pregunta (sin la respuesta)
    console.log(`\n--- ¡EL CONCURSO COMIENZA AHORA! (${TIEMPO_LIMITE_SEG}s para responder) ---\n`);

    ESTUDIANTES.forEach((estudianteId) => {
      const { categoria, pregunta } = asignacion.get(estudianteId);
      tiemposInicio.set(estudianteId, Date.now());

      channel.publish(exchangeDirect, estudianteId, Buffer.from(`[${categoria}] ${pregunta}`), {
        correlationId: estudianteId,
        replyTo: replyQueue.queue,
        expiration: String(TIEMPO_LIMITE_SEG * 1000), // RabbitMQ descarta la pregunta si no se consume a tiempo
        mandatory: true,                              // avisa (evento 'return') si el estudiante no está conectado
      });
    });

    // 3. Cerrar el concurso y anunciar resultados por el FANOUT
    function cerrarConcurso() {
      if (concursoCerrado) return;
      concursoCerrado = true;

      console.log('\n--- CONCURSO CERRADO ---');
      const encabezado = ganador
        ? `🏆 ¡EL GANADOR ES ${ganador.toUpperCase()}! Respondió correctamente en ${tiempoGanador}s`
        : '❌ Nadie respondió correctamente a tiempo. No hay ganador.';

      const detalle = ESTUDIANTES.map((id) => {
        const { pregunta, respuesta: correcta } = asignacion.get(id);
        const r = resultados.get(id);
        const estado = ausentes.has(id) ? '🔌 no conectado'
          : !r ? '— sin respuesta'
          : `${r.correcta ? '✔' : '✘'} "${r.respuesta}" (${r.motivo}, ${r.tiempo}s)`;
        return `  • ${id}: ${estado}\n      ${pregunta} → ${correcta}`;
      }).join('\n');

      const mensajeFinal = `${encabezado}\n\nResultados:\n${detalle}`;
      console.log(mensajeFinal);

      // 4. NOTIFICAR A TODOS (Fanout)
      channel.publish(exchangeFanout, '', Buffer.from(mensajeFinal));

      setTimeout(() => { connection.close(); process.exit(0); }, 2000);
    }

    temporizador = setTimeout(cerrarConcurso, (TIEMPO_LIMITE_SEG + 1) * 1000);
  } catch (error) {
    console.error('Error:', error);
    if (connection) await connection.close();
  }
}

module.exports = { normalizar, validarRespuesta, BANCO_PREGUNTAS }; // para pruebas

if (require.main === module) enviarPreguntas();