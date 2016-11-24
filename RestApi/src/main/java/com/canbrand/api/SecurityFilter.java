//package com.canbrand.api;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.StringTokenizer;
//
//import javax.ws.rs.container.ContainerRequestContext;
//import javax.ws.rs.container.ContainerRequestFilter;
//import javax.ws.rs.core.Response;
//import javax.ws.rs.ext.Provider;
//
//import org.glassfish.jersey.internal.util.Base64;
//
//@Provider
//public class SecurityFilter implements ContainerRequestFilter {
//
//	private static final String AUTHORIZATION_HEADER_KEY = "Authorization";
//	private static final String AUTHORIZATION_HEADER_PRIFIX = "Basic ";
//	private static final String SECURED_URL_PRIFIX = "secured";
//
//	@Override
//	public void filter(ContainerRequestContext requestContext) throws IOException {
//
//		if (requestContext.getUriInfo().getPath().contains(SECURED_URL_PRIFIX)) {
//			List<String> authHeader = requestContext.getHeaders().get(AUTHORIZATION_HEADER_KEY);
//			if (authHeader != null && authHeader.size() > 0) {
//				String authToken = authHeader.get(0);
//				authToken = authToken.replaceFirst(AUTHORIZATION_HEADER_PRIFIX, "");
//				String string = Base64.decodeAsString(authToken);
//				StringTokenizer stringTokenizer = new StringTokenizer(string, ":");
//				String username = stringTokenizer.nextToken();
//				String password = stringTokenizer.nextToken();
//				if ("shahzad".equals(username) && "password".equals(password)) {
//					return;
//				}
//			}
//			Response unauthorized = Response.status(Response.Status.UNAUTHORIZED)
//					.entity("You are not eligible to access the resource").build();
//			requestContext.abortWith(unauthorized);
//		}
//	}
//
//}
