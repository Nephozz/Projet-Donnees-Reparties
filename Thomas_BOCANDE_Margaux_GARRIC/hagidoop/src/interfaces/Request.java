package interfaces;

import java.io.Serializable;

/*
 * Définition d'une requête qui sera envoyée au serveur
 * Permet de gérer la lecture, l'écriture et la suppression de fichiers
 * sous la forme de teste ou de KV
 */
public class Request implements Serializable {
    public RequestType type;

    // Format du fichier à écrire
    public int fmt;
    // Nom du fichier
    public String fname;

    // Contenu du fichier à lire/écrire
    public Object content;
    // Formats autorisés
    private final boolean ALLOWED_FORMATS = this.content instanceof String && this.content instanceof KV && this.content instanceof KV[];

    public Request() {}

    public Request(RequestType type, String fname) {
        super();
        this.type = type;
        this.fname = fname;
    }

    // Permet de définir le format du fichier à écrire
    public void setFmt(int fmt) {
        if (this.type != RequestType.WRITE) {
            System.out.println("Cannot set format for non-write request");
            return;
        }

        if (fmt != FileReaderWriter.FMT_TXT && fmt != FileReaderWriter.FMT_KV) {
            System.out.println("Unknown format: " + fmt);
            return;
        }
        
        this.fmt = fmt;
    }

    // Passe le contenu du fichier à écrire
    public void passContent(Object object) {
        if (this.type != RequestType.WRITE) {
            System.out.println("Cannot set content for non-write request");
            return;
        }

        if (!ALLOWED_FORMATS) {
            System.out.println("Unknown content type: " + object.getClass().getName());
            return;
        }

        if (this.fmt == FileReaderWriter.FMT_TXT && !(object instanceof String)) {
            System.out.println("Content must be of type String");
            return;
        } else if (this.fmt == FileReaderWriter.FMT_KV && !(object instanceof KV || object instanceof KV[])) {
            System.out.println("Content must be of type KV");
            return;
        }

        this.content = object;
    }
}
