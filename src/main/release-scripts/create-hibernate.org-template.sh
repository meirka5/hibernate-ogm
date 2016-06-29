echo "##################################################"
echo "# Hibernate OGM release $RELEASE_VERSION step 2/9 "
echo "# `date`"
echo "# Creating template for release announcement file"
echo "##################################################"

<<COMMENT1
cat <<EOF | ruby
    require 'rubygems'
    require 'json'
    require 'net/https'

    releaseVersion = "$RELEASE_VERSION"

    uri = URI.parse("https://hibernate.atlassian.net/rest/api/latest/project/OGM/versions")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    request = Net::HTTP::Get.new(uri.request_uri)
    response = http.request(request).body

    parsed = JSON.parse(response)

    version = parsed.find{|version| version["name"] == releaseVersion }
    if !version.nil?
        puts "Run the following command in the hibernate.org repository to create the release announcement file:"
        puts ""
        puts "====="
        puts "cat <<EOF > _data/projects/ogm/releases/" + version["name"] + ".yml" 
        puts "version: " + version["name"]
        puts "version_family: " + releaseVersion.split('.')[0] + "." + releaseVersion.split('.')[1]
        puts "date: " + version["releaseDate"]
        puts "stable: " + (releaseVersion.include?("Final") ? "true" : "false")
        puts "announcement_url: <TBD>"
        puts "summary: " + (version["description"] or "<TBD>")
        puts "displayed: true"
        puts "EOF"
        puts "====="
    else
        puts "WARNING: Version $RELEASE_VERSION does not exist in JIRA"
   end
EOF
COMMENT1


