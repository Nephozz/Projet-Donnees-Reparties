package hdfs;

import java.io.Serializable;
import java.util.List;

import interfaces.KV;

/*
 * Défiintion des différents types de requêtes
 */
enum RequestType {
    READ, WRITE, DELETE
}

/*
 * Définition d'une requête qui sera envoyée au serveur
 * Permet de gérer la lecture, l'écriture et la suppression de fichiers
 * sous la forme de teste ou de KV
 */
public class Request implements Serializable {

    public RequestType type;

    // Nom du fichier
    public String fname;

    // Contenu du fichier à lire/écrire
    public Object content;

    // Formats autorisés
    //private static final boolean ALLOWED_FORMATS = 
    //    content instanceof KV && ((List<?>) content).stream().allMatch(element -> element instanceof KV);

    public Request() {}

    public Request(RequestType type, String fname) {
        super();
        this.type = type;
        this.fname = fname;
    }

    // Passe le contenu du fichier à écrire
    public void passContent(Object object) {
        //if (this.type != RequestType.WRITE) {
        //    System.out.println("Cannot set content for non-write request");
        //    return;
        //}

        //if (!ALLOWED_FORMATS) {
        //    System.out.println("Unknown content type: " + object.getClass().getName());
        //    return;
        //}

        content = object;
    }
}
