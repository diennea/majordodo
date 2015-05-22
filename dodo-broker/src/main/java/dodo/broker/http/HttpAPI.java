/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dodo.broker.http;

import javax.ws.rs.Path;

@Path("/")
public class HttpAPI {


    @Path("/client")
    public ClientAPI client() {
        return new ClientAPI();
    }
}
