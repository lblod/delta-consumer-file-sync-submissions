import fetch from 'node-fetch';
import {
  SECRET_KEY, SYNC_LOGIN_ENDPOINT
} from '../config';


let cookie = null;
async function login() {
  try {
    const resp = await fetch(SYNC_LOGIN_ENDPOINT, {
      headers: {
        'key': process.env.SECRET_KEY,
        'accept': "application/vnd.api+json"
      },
      method: 'POST'
    });

    if (!resp.ok) {
      console.log("FAILED TO LOG IN");
      throw "Could not log in";
    }

    if (resp.headers.raw()['set-cookie']) {
      const [setCookie,] = resp.headers.raw()['set-cookie'];
      const [newCookie,] = setCookie.split(';');
      console.log(`GOT COOKIE, SETTING ${newCookie}`);
      cookie = newCookie;
      // this.nextCookie = cookie;
      // console.log(`We logged in to ${SYNC_LOGIN_ENDPOINT} cookie ${this.nextCookie}`);
    }
  } catch (e) {
    console.log(`Something went wrong while logging in at ${SYNC_LOGIN_ENDPOINT}`);
    console.log(e);
    throw e;
  }
}

export default async function fetcher(url, options, isRetry = false) {
  if(!SYNC_LOGIN_ENDPOINT){
    return await fetch(url, options);
  } else {

    if( !cookie ) {
      await login();
    } else {
      console.log("FETCH WITH COOKIE");
    }

    // do fetch call as usual but add cookie
    const fetchOptions = Object.assign( {}, options || {} );
    fetchOptions.headers = fetchOptions.headers || {};
    fetchOptions.headers.cookie = cookie;

    // send fetch
    console.log(`Going to send fetch with ${JSON.stringify(fetchOptions)}`);

    let resp = await fetch(url, fetchOptions);

    if(resp.status == 400 && !isRetry){
      cookie = null;
      return await fetcher(url, options, true);
    }
    else {

      // extract new cookie if provided and set it
      if( resp.headers.raw()['set-cookie'] ) {
        const [setCookie,] = resp.headers.raw()['set-cookie'];
        const [newCookie,] = setCookie.split(';');
        console.log(`GOT COOKIE, SETTING ${newCookie}`);
        cookie = newCookie;
      }

    }

    return resp;
  }
}
