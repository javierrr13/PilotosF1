import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.bson.Document;
import org.bson.conversions.Bson;

public class AppMongoDB {

	private MongoDatabase database;

	public AppMongoDB() {
		MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
		database = mongoClient.getDatabase("PilotosF1");
	}

	public void crearColeccionDesdeArchivo(String nombreColeccion, String rutaArchivo) {
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
		} else {
			System.out.println("No se encontraron datos para el piloto: " + nombrePiloto);
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
	    	Document mejorPiloto = collection.find()
	                                     .sort(Sorts.descending("grandes_premios"))
	                                     .first();

	    if (mejorPiloto != null) {
	    	System.out.println("");
	        System.out.println("El piloto con más Grandes Premios ganados es: ");
	        System.out.println("Nombre: " + mejorPiloto.getString("nombre"));
	        System.out.println("Nacionalidad: " + mejorPiloto.getString("nacionalidad"));
	        System.out.println("Edad: " + mejorPiloto.getInteger("edad"));
	        System.out.println("Escudería: " + mejorPiloto.getString("escuderia"));
	        System.out.println("Grandes Premios: " + mejorPiloto.getInteger("grandes_premios"));
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
	    } else {
	        System.out.println("Nuevo valor (entero):");
	        nuevoValor = scanner.nextInt();
	        scanner.nextLine(); 
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
		String nombreColeccion = "PilotosF1";
		Scanner scanner = new Scanner(System.in);

		System.out.println("[-] Bienvenido al sistema de gestión de pilotos de F1 [-]");
		System.out.println("1: Insertar piloto");
		System.out.println("2: Eliminar piloto");
		System.out.println("3: Consultar datos de un piloto");
		System.out.println("4: Actualizar datos piloto ya existente");
		System.out.println("5: Extraer mejor piloto de la coleccion");
		System.out.println("6: Creaccion de coleccion de pilotos");
		System.out.println("7: Consulta todos los pilotos ");
		System.out.println("---------------Elige una opción---------------");

		int opcion = scanner.nextInt();
		scanner.nextLine();

		switch (opcion) {
		case 1:
			app.insertarPilotoPorTeclado(nombreColeccion);
			break;
		case 2:
			System.out.println("Introduce el nombre del piloto a eliminar:");
			String nombre = scanner.nextLine();
			app.eliminarPilotoPorNombre(nombreColeccion, nombre);
			break;
		case 3:
			System.out.println("Introduce el nombre del piloto a consultar:");
			String nombrePiloto = scanner.nextLine();
			app.consultarDatosPiloto(nombreColeccion, nombrePiloto);
			break;
		case 4 :
			app.actualizarDatosPiloto(nombreColeccion);
			break;
		case 5 : 
			app.extraerMejorPiloto(nombreColeccion);
			break;
		case 6 : 
			System.out.println("Especifica ruta .csv");
			String path = scanner.nextLine();
			app.crearColeccionDesdeArchivo(nombreColeccion, System.getProperty("user.home")+path);
			break;
		case 7 : 
			app.consultarTodosLosPilotos(nombreColeccion);
			break;
		default:
			System.out.println("Opción no válida");
			break;
		}

		scanner.close();
	}
}
