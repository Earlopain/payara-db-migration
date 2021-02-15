package net.c5h8no4na.migration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.c5h8no4na.common.Tuple;
import net.c5h8no4na.e621.api.response.PostApi;
import net.c5h8no4na.e621.api.response.partial.File;
import net.c5h8no4na.e621.api.response.partial.Relationships;
import net.c5h8no4na.e621.api.response.partial.Score;
import net.c5h8no4na.e621.api.response.partial.Tags;
import net.c5h8no4na.entity.e621.Post;
import net.c5h8no4na.entity.e621.Tag;
import net.c5h8no4na.entity.e621.enums.Extension;
import net.c5h8no4na.entity.e621.enums.TagType;

@Path("/")
public class Test {

	@Inject
	private Backend backend;

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String test(@QueryParam("id") Integer id) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		return gson.toJson(postToApiPost(backend.findOrCreatePost(id)));
	}

	@GET
	@Path("/file")
	public Response getFile(@QueryParam("id") Integer id) {
		Optional<Tuple<byte[], Extension>> file = backend.getPostFile(id);
		if (file.isEmpty()) {
			return Response.serverError().status(Status.NOT_FOUND).build();
		} else {
			return Response.ok(file.get().x).type(file.get().y.toMediaType()).build();
		}
	}

	private PostApi postToApiPost(Post post) {
		if (post == null) {
			return null;
		}
		PostApi p = new PostApi();
		p.setId(post.getId());
		p.setCreatedAt(post.getCreatedAt());
		p.setUpdatedAt(post.getUpdatedAt());

		File f = new File();
		f.setWidth(post.getWidth());
		f.setHeight(post.getHeight());
		f.setExt(post.getExtension().name().toLowerCase());
		f.setSize(post.getSize());
		f.setMd5(post.getMd5());
		p.setFile(f);

		Score s = new Score();
		s.setUp(post.getScoreUp());
		s.setDown(post.getScoreDown());
		s.setTotal(post.getScoreTotal());
		p.setScore(s);

		List<Tag> tags = post.getTags();

		Tags t = new Tags();
		t.setArtist(tags.stream().filter(tag -> tag.getTagType() == TagType.ARTIST).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setCharacter(
				tags.stream().filter(tag -> tag.getTagType() == TagType.CHARACTER).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setCopyright(
				tags.stream().filter(tag -> tag.getTagType() == TagType.COPYRIGHT).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setGeneral(
				tags.stream().filter(tag -> tag.getTagType() == TagType.GENERAL).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setInvalid(
				tags.stream().filter(tag -> tag.getTagType() == TagType.INVALID).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setLore(tags.stream().filter(tag -> tag.getTagType() == TagType.LORE).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setMeta(tags.stream().filter(tag -> tag.getTagType() == TagType.META).map(tag -> tag.getText()).collect(Collectors.toList()));
		t.setSpecies(
				tags.stream().filter(tag -> tag.getTagType() == TagType.SPECIES).map(tag -> tag.getText()).collect(Collectors.toList()));
		p.setTags(t);

		p.setRating(Character.toString(post.getRating().name().toLowerCase().charAt(0)));
		p.setFavCount(post.getFavCount());

		Relationships r = new Relationships();
		r.setChildren(post.getChildren().stream().map(c -> c.getId()).collect(Collectors.toList()));
		r.setParentId(post.getParent().isPresent() ? post.getParent().get().getId() : null);
		p.setRelationships(r);

		p.setApproverId(post.getApprover().isPresent() ? post.getApprover().get().getId() : null);
		p.setUploaderId(post.getUploader().getId());
		p.setDescription(post.getDescription());
		p.setDuration(post.getDuration().orElse(null));
		p.setSources(post.getSources().stream().map(source -> source.getUrl()).collect(Collectors.toList()));

		return p;
	}
}