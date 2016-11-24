package com.canbrand.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.canbrand.model.Person;

@Path("api")
public class AddUserController {

	@Path("addUser/{firstName}/{lastName}/{age}/{address}/{houseno}/{pinno}")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Person userList(@PathParam("firstName") String firstName, @PathParam("lastName") String lastName,
			@PathParam("age") int age, @PathParam("address") String address, @PathParam("houseno") String houseno,
			@PathParam("pinno") String pinno) {
		Person person = new Person();
		person.setFirstName(firstName);
		person.setLastName(lastName);
		person.setAge(age);
		person.setAddress(address);
		person.setHouseno(houseno);
		person.setPinno(pinno);
		return person;

	}

}
