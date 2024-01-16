package daemon;

import config.Project;

public class Hagidoop {
    public static void main(String args[]) {
        Project projet = new Project();

        projet.setPath(args[0]);
    }
}