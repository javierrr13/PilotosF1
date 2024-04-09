import com.mongodb.client.MongoClients;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.eq;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;
import org.bson.conversions.Bson;

public class AppMongoDB {

	private MongoDatabase database;

	public AppMongoDB() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		database = mongoClient.getDatabase("PilotosF1");
	}

	public void crearColeccionPilotos(String nombreColeccion, String rutaArchivo) {
		MongoCollection<Document> collection = database.getCollection(nombreColeccion);

		try (BufferedReader br = new BufferedReader(new FileReader(rutaArchivo))) {
			String linea;
			boolean primeraLinea = true;

			while ((linea = br.readLine()) != null) {
				if (primeraLinea) {
					primeraLinea = false;
					continue;
				}

				String[] atributos = linea.split(",");
				if (atributos.length == 5) {
					Document doc = new Document("nombre", atributos[0]).append("nacionalidad", atributos[1])
							.append("edad", Integer.parseInt(atributos[2])).append("escuderia", atributos[3])
							.append("grandes_premios", Integer.parseInt(atributos[4]));
					collection.insertOne(doc);
				}
			}
			System.out.println("");
			System.out.println("[+] Coleccion creada con exito [+]");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void crearColeccionEscuderias(String nombreColeccion, String rutaArchivo) {
        MongoCollection<Document> collection = database.getCollection(nombreColeccion);

        try (BufferedReader br = new BufferedReader(new FileReader(rutaArchivo))) {
            String linea;
            boolean primeraLinea = true;

            while ((linea = br.readLine()) != null) {
                if (primeraLinea) {
                    primeraLinea = false;
                    continue;
                }

                String[] atributos = linea.split(",");
                if (atributos.length == 3) {
                    Document doc = new Document("nombre", atributos[0]).append("pais", atributos[1])
                            .append("director", atributos[2]);
                    collection.insertOne(doc);
                }
            }
            System.out.println("");
            System.out.println("[+] Colección de escuderías creada con éxito [+]");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void consultarDatosPiloto(String nombreColeccion, String nombrePiloto) {
        MongoCollection<Document> collection = database.getCollection(nombreColeccion);

        Document filtro = new Document("nombre", nombrePiloto);
        Document piloto = collection.find(filtro).first();

        if (piloto != null) {
            System.out.println("");
            System.out.println("[+] Datos del piloto [+]");
            System.out.println("-------------------------------------------");
            System.out.println("Nombre: " + piloto.getString("nombre"));
            System.out.println("Nacionalidad: " + piloto.getString("nacionalidad"));
            System.out.println("Edad: " + piloto.getInteger("edad"));
            System.out.println("Escudería: " + piloto.getString("escuderia"));
            System.out.println("Grandes Premios: " + piloto.getInteger("grandes_premios"));
            
            consultarDatosEscuderia(piloto.getString("escuderia"));
        } else {
            System.out.println("No se encontraron datos para el piloto: " + nombrePiloto);
        }
    }

    public void consultarDatosEscuderia(String nombreEscuderia) {
        MongoCollection<Document> collection = database.getCollection("EscuderiasF1");

        Document filtro = new Document("nombre", nombreEscuderia);
        Document escuderia = collection.find(filtro).first();

        if (escuderia != null) {
            System.out.println("");
            System.out.println("[+] Datos de la escudería [+]");
            System.out.println("-------------------------------------------");
            System.out.println("Nombre: " + escuderia.getString("nombre"));
            System.out.println("País: " + escuderia.getString("pais"));
            System.out.println("Director: " + escuderia.getString("director"));
        } else {
            System.out.println("No se encontraron datos para la escudería: " + nombreEscuderia);
        }
    }

	public void insertarPilotoPorTeclado(String nombreColeccion) {
		MongoCollection<Document> collection = database.getCollection(nombreColeccion);
		Scanner scanner = new Scanner(System.in);

		System.out.println("Introduce los datos del nuevo piloto.");

		System.out.print("Nombre: ");
		String nombre = scanner.nextLine();

		System.out.print("Nacionalidad: ");
		String nacionalidad = scanner.nextLine();

		System.out.print("Edad: ");
		int edad;
		while (true) {
			try {
				edad = Integer.parseInt(scanner.nextLine());
				break;
			} catch (NumberFormatException e) {
				System.out.print("Por favor, introduce un número válido para la edad: ");
			}
		}

		System.out.print("Escudería: ");
		String escuderia = scanner.nextLine();

		System.out.print("Grandes Premios: ");
		int grandesPremios;
		while (true) {
			try {
				grandesPremios = Integer.parseInt(scanner.nextLine());
				break;
			} catch (NumberFormatException e) {
				System.out.print("Por favor, introduce un número válido para los grandes premios: ");
			}
		}

		Document nuevoPiloto = new Document("nombre", nombre).append("nacionalidad", nacionalidad).append("edad", edad)
				.append("escuderia", escuderia).append("grandes_premios", grandesPremios);

		collection.insertOne(nuevoPiloto);
		System.out.println("Piloto insertado correctamente.");

	}

	public void extraerMejorPiloto(String nombreColeccion) {
	    MongoCollection<Document> collection = database.getCollection(nombreColeccion);
	    FindIterable<Document> mejoresPilotos = collection.find()
	            .sort(Sorts.descending("grandes_premios"))
	            .limit(1); 
	    
	    List<Document> pilotos = new ArrayList<>();
	    Document mejorPiloto = mejoresPilotos.first();

	    if (mejorPiloto != null) {
	        int maxGrandesPremios = mejorPiloto.getInteger("grandes_premios");
	        FindIterable<Document> pilotosMaxGP = collection.find(eq("grandes_premios", maxGrandesPremios));
	        pilotosMaxGP.into(pilotos);

	        if (!pilotos.isEmpty()) {
	            System.out.println("");
	            System.out.println("Los pilotos con más Grandes Premios ganados son:");

	            for (Document piloto : pilotos) {
	            	int count = 0;
	            	count++;
	            	System.out.println("["+ count +"]");
	                System.out.println("Nombre: " + piloto.getString("nombre"));
	                System.out.println("Nacionalidad: " + piloto.getString("nacionalidad"));
	                System.out.println("Edad: " + piloto.getInteger("edad"));
	                System.out.println("Escudería: " + piloto.getString("escuderia"));
	                // Ahora también obtenemos y mostramos la información de la escudería
	                consultarDatosEscuderia(piloto.getString("escuderia"));
	                System.out.println("Grandes Premios: " + piloto.getInteger("grandes_premios"));
	                System.out.println("-------------------------------------------");
	            }
	        } else {
	            System.out.println("No se encontraron pilotos en la colección.");
	        }
	    } else {
	        System.out.println("No se encontraron pilotos en la colección.");
	    }
	}




	public void actualizarDatosPiloto(String nombreColeccion) {
	    Scanner scanner = new Scanner(System.in);

	    System.out.println("Piloto a actualizar:");
	    String nombrePiloto = scanner.nextLine();

	    System.out.println("Dato a actualizar:");
	    String campo = scanner.nextLine();

	    Object nuevoValor = null;

	    if(campo.equals("nombre") || campo.equals("nacionalidad") || campo.equals("escuderia")) {
	        System.out.println("Nuevo valor:");
	        nuevoValor = scanner.nextLine();
	    }if(campo.equals("edad")||campo.equals("grandes premios")){
	        System.out.println("Nuevo valor (entero):");
	        nuevoValor = scanner.nextInt();
	        scanner.nextLine(); 
	    }else {
	    	System.out.println("[-] Valor introducido no valido [-]");
	    }

	    MongoCollection<Document> collection = database.getCollection(nombreColeccion);

	    Bson filtro = new Document("nombre", nombrePiloto);
	    Bson actualizacion = new Document("$set", new Document(campo, nuevoValor));

	    UpdateResult result = collection.updateOne(filtro, actualizacion);

	    if (result.getModifiedCount() > 0) {
	        System.out.println("Datos actualizados correctamente.");
	    } else {
	        System.out.println("No se ha podido actualizar los datos.");
	    }
	}

	public void eliminarPilotoPorNombre(String nombreColeccion, String nombrePiloto) {
		MongoCollection<Document> collection = database.getCollection(nombreColeccion);

		Document filtro = new Document("nombre", nombrePiloto);
		DeleteResult result = collection.deleteMany(filtro);
		System.out.println("Número de pilotos eliminados: " + result.getDeletedCount());
	}
	public void consultarTodosLosPilotos(String nombreColeccion) {
	    MongoCollection<Document> collection = database.getCollection(nombreColeccion);

	    try (MongoCursor<Document> cursor = collection.find().iterator()) {
	        if (!cursor.hasNext()) {
	            System.out.println("No hay pilotos en la colección.");
	        }
	        while (cursor.hasNext()) {
	            Document doc = cursor.next();
	            System.out.println("Nombre: " + doc.getString("nombre"));
	            System.out.println("------------------------------------");
	        }
	    }
	}

	public static void main(String[] args) {
		AppMongoDB app = new AppMongoDB();
		String ColeccionPilotos = "PilotosF1";
		String ColeccionEscuderias = "EscuderiasF1";
		Scanner scanner = new Scanner(System.in);
		Boolean flag = true;
		System.out.println("[-] Bienvenido al sistema de gestión de pilotos de F1 [-]");
	while(flag) {
		System.out.println("\n");
		System.out.println("1: Insertar piloto");
		System.out.println("2: Eliminar piloto");
		System.out.println("3: Consultar datos de un piloto");
		System.out.println("4: Actualizar datos piloto ya existente");
		System.out.println("5: Extraer mejor piloto de la coleccion");
		System.out.println("6: Creaccion de coleccion de pilotos");
		System.out.println("7: Consulta todos los pilotos ");
		System.out.println("8: Salir del programa ");
		System.out.println("9: Crear coleccion escuderias");
		System.out.println("---------------Elige una opción---------------");

		int opcion = scanner.nextInt();
		scanner.nextLine();

		switch (opcion) {
		case 1:
			app.insertarPilotoPorTeclado(ColeccionPilotos);
			break;
		case 2:
			System.out.println("Introduce el nombre del piloto a eliminar:");
			String nombre = scanner.nextLine();
			app.eliminarPilotoPorNombre(ColeccionPilotos, nombre);
			break;
		case 3:
			System.out.println("Introduce el nombre del piloto a consultar:");
			String nombrePiloto = scanner.nextLine();
			app.consultarDatosPiloto(ColeccionPilotos, nombrePiloto);
			break;
		case 4 :
			app.actualizarDatosPiloto(ColeccionPilotos);
			break;
		case 5 : 
			app.extraerMejorPiloto(ColeccionPilotos);
			break;
		case 6 : 
			System.out.println("Especifica ruta .csv");
			String path = scanner.nextLine();
			app.crearColeccionPilotos(ColeccionPilotos, System.getProperty("user.home")+ path);
			break;
		case 7 : 
			app.consultarTodosLosPilotos(ColeccionPilotos);
			break;
		case 8:
			flag = false;
			break;
		case 9: 
			System.out.println("Especifica ruta .csv");
			String route = scanner.nextLine();
			app.crearColeccionEscuderias(ColeccionEscuderias,System.getProperty("user.home") + route);
			break;
		default:
			System.out.println("Opción no válida");
			break;
		}

	}
		System.out.println("[-] Programa finalizado ");
		scanner.close();
	}
}
