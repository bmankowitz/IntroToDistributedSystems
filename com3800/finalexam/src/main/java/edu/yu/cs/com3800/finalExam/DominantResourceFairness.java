package edu.yu.cs.com3800.finalExam;

import java.util.*;

/**
 * Implements a single run of the DRF algorithm.
 */
public class DominantResourceFairness {
    private final boolean DEBUG = false;

    /**
     * Describes the allocation of units of a single resource to a user
     */
    public class Allocation{
        String resourceName;
        double unitsAllocated;
        User user;
        public Allocation(String resourceName, double unitsAllocated, User user){
            this.resourceName = resourceName;
            this.unitsAllocated = unitsAllocated;
            this.user = user;
        }
        public String toString(){
            return this.unitsAllocated + " of " + this.resourceName + " allocated to " + this.user;
        }
    }

    /**a map of the resources that exist in this system. Key is the resource's name, value is the actial resource object*/
    private Map<String,SystemResource> systemResources;
    /**Users in the system, sorted by their dominant share. Note: to re-sort the users in the TreeSet,
     * when a User's dominant share changes, you must remove it from the TreeSet and re-add it*/
    private TreeSet<User> users;

    /**
     * @param systemResources
     * @param users
     * @throws IllegalArgumentException if either collection is empty or null
     */
    public DominantResourceFairness(Map<String,SystemResource> systemResources,TreeSet<User> users){
        this.systemResources = systemResources;
        this.users = new TreeSet<>(Comparator.comparing(User::getDominantShare).thenComparing(User::toString));
        for(User user : users){
            this.users.add(user);
        }
        if(DEBUG) {
            System.out.println("Received Resources: ");
            systemResources.forEach((x, y) -> System.out.println("\t" + x + "  ->  " + y.getUnits()));
            System.out.println("Received Users: " + users);
            users.forEach(x -> System.out.println("\t"+x));
        }
    }

    /**
     * Repeatedly allocate resources to the user with the lowest dominant share, until there are
     * insufficient unallocated resources remaining to meet any user's requirements.
     * @return a list of the individual resource allocations made by DRF, in order
     */
    public List<Allocation> allocateResources() {
        ArrayList<Allocation> allocations = new ArrayList<>();
        User user;
        while(ableToProcessRequest(user = users.first())){
            allocations.addAll(processRequest(user));
            users.remove(user);
            users.add(user);
        };
        return allocations;
    }
    private List<Allocation> processRequest(User user){
        ArrayList<Allocation> allocations = new ArrayList<>();
        Map<String, Resource> request = user.getRequiredResourcesPerTask();
        for (Map.Entry<String, Resource> entry : request.entrySet()) {
            String type = entry.getKey();
            Resource amount = entry.getValue();
            systemResources.get(type).allocate(amount.getUnits());
            user.allocateResources(request.values());
            if(DEBUG) System.out.println(user + " -> " + type + ": Allocated " + amount.getUnits() + " of " + type + ". Remaining: "
                    + systemResources.get(type).getAvailable() + " of original " + systemResources.get(type).getUnits());
            //1.0 of cpu allocated to User A
            allocations.add(new Allocation(type, amount.getUnits(), user));
        }
        return allocations;
    }

    private boolean ableToProcessRequest(User user){
        Map<String, Resource> request = user.getRequiredResourcesPerTask();
        boolean able = false;
        for (Map.Entry<String, Resource> entry : request.entrySet()) {
            String name = entry.getKey();
            Resource amount = entry.getValue();

            if (systemResources.get(name) == null){
                if(DEBUG) System.out.println(name+": unable to allocate: "+name+" does not exist");
                return false;
            }
            if(systemResources.get(name).getAvailable() - amount.getUnits() < 0){
                if(DEBUG) System.out.println(name+": unable to allocate: insufficient units available. Available: "
                        +systemResources.get(name).getAvailable() +" . Requested: "+amount.getUnits());
                return false;
            }
            else{
                able = true;
                if(DEBUG) System.out.println(name+ ": Able to allocate " +amount.getUnits() +" units of available "
                        +systemResources.get(name).getAvailable());
            }
        }
        if(able){
            if(DEBUG) System.out.println("Able to allocate entire request");
            return true;
        }
        else{
            if(DEBUG) System.out.println("Not able to allocate");
            return false;
        }
    }
}
